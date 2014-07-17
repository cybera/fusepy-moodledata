import os, time, dateutil.parser, errno
import logging
from collections import deque
from shutil import copyfile
from stat import S_IFDIR, S_IFLNK, S_IFREG
from threading import Lock
import thread

from fuse import FUSE, FuseOSError, Operations, LoggingMixIn, fuse_get_context

from swift_source import SwiftSource
from fsnode import FSNode
import file_system_cache_init

class FileSystem(LoggingMixIn, Operations):
	def __init__(self, config):
		self.logger = logging.getLogger('fuse')
		self.rwlock = Lock()

		self.config = config
		self.cache_root = os.path.realpath(config["cache_dir"])

		self.swift_connection = SwiftSource(
			auth_url=config["swift.auth_url"],
			username=config["swift.username"],
			password=config["swift.password"],
			tenant_id=config["swift.tenant_id"],
			region_name=config["swift.region_name"],
			source_bucket=config["source_bucket"])

		FSNode.set_swift_connection(self.swift_connection)
		if self.config["metadata_collection"] == "prefetch":
			self.refresh_from_object_store()
		else:
			# initialize cache
			node = FSNode()
			node.update_from_cache("/", self.cache_path("/"))
			node.save()
		
		self.pending_operations = deque()
		self.job_executor_thread = thread.start_new_thread(self._job_executor_thread_main, ())

	####### FUSE Functions #######

	### Fuse functions that need to work with the object store

	def chmod(self, path, mode):
		node = self.get(path)
		node.mode = mode
		node.save()
		# TODO: This has the potential to be quite slow. We may want to send the
		# operation off to a background process
		### obj = self.swift_connection.get_object(path)
		### obj.set_metadata({ "fs-mode": "%i" % mode })
		return 0

	def chown(self, path, uid, gid):
		node = self.get(path)
		node.uid = uid
		node.gid = gid
		node.save()
		# TODO: This has the potential to be quite slow. We may want to send the
		# operation off to a background process
		### obj = self.swift_connection.get_object(path)
		### obj.set_metadata({ "fs-uid": "%i" % uid, "fs-gid": "%i" % gid })
		return 0

	def getattr(self, path, fh=None):
		node = self.get(path)
		if node:
			return node.attr()
		else:
			# TODO: Certain types of operations (mkdir, for example), cause this to be called on a path
			# that does not yet exist. What do we do about it?
			st = os.lstat(self.cache_path(path))
			return dict((key, getattr(st, key)) for key in ('st_atime', 'st_ctime',
				'st_gid', 'st_mode', 'st_mtime', 'st_nlink', 'st_size', 'st_uid'))

	def mkdir(self, path, mode):
		# TODO: Handle existing directory
		os.mkdir(self.cache_path(path), mode)
		node = self.get_or_create(path)
		node.update_from_cache(path, self.cache_path(path))
		node.save()

		def callback(success, error_message):
			pass

		def pre_execution():
			pass
				
		args = (node, self.cache_root, callback)
		operation = FileOperation(path, self.swift_connection.update_object, args, pre_execution)
		self.pending_operations.append(operation)

		return 0

	def read(self, path, size, offset, fh):
		"""
		Returns the chunk of the file specified.
		If the file is currently downloading we wait until the request can be fulfilled
		"""
		node = self.get(path)
		cached_file = self.cache_path(path)
		while node.downloading != None:
			if os.path.getsize(cached_file) >= offset + size:
				break
			# TODO: the sleep time should maybe be customizable via config file
			# TODO: we should probably have a timeout, this should be a function of the file size
			#       and time elapsed for download
			time.sleep(0.1)
			node = self.get(path) # refresh node object from db
		if fh:
			with self.rwlock:
				os.lseek(fh, offset, 0)
				return os.read(fh, size)
		else:
			raise FuseOSError(errno.ENOENT)

	def readdir(self, path, fh):
		fsnode = self.get(path)

		if fsnode:
			return [child_node.name for child_node in fsnode.children()]
		else:
			raise FuseOSError(errno.ENOENT)

	def write(self, path, data, offset, fh):
		# Make sure the path exists to grab the file to.
		# TODO: make sure to set mode/gid/uid and various times according to what's in the metadata db
		# as all of these folders should actually have objects and thus database items for them.
		cache_folder_path = os.path.dirname(self.cache_path(path))
		if not os.path.exists(cache_folder_path):
			os.makedirs(cache_folder_path)

		node = self.get(path)

		os.lseek(fh, offset, 0)
		retval = os.write(fh, data)

		if node:
			node.dirty = 1
			node.save()

		node.update_from_cache(path, self.cache_path(path))
		# Strange things happen if you don't return the number of bytes written from this function call.
		return retval

	def release(self, path, fh):
		if fh:
			os.close(fh)

		node = self.get(path)
		def callback(success, error_message):
			node = self.get(path)
			if node is None:
				self.logger.error("In the callback of release, node should not be none, but it is")
				return
			if node.dirty == 1 and node.uploading is not None:
				node.uploading = None
				if success:
					node.dirty = 0
				node.save()
				if not success:
					self.logger.error("Upload failed, trying again")
					self.release(path, fh)
			elif node.dirty == 1 and node.uploading is None:
				self.release(path, fh)
			else:
				# TODO: do we need to do anything if we have an unexpected value for dirty or uploading?
			  #       I don't think that it's possible to have dirty == 0 and uploading == 1, but what about the case
				#       that both are 0? Does this possibly indicate that we are in an unexpected state?
				pass

		def pre_execution():
			node.uploading = time.time()
			node.save()
				
		if node and node.dirty == 1:
			node.update_from_cache(path, self.cache_path(path))
			node.save()
			args = (node, self.cache_root, callback)
			operation = FileOperation(path, self.swift_connection.update_object, args, pre_execution)
			self.pending_operations.append(operation)
		return 0

	def symlink(self, target, source):
		# TODO: Handle existing symbolic link
		def callback(success, error_message):
			# TODO: implement callback
			pass
		def pre_execution():
			pass
				
		os.symlink(source, self.cache_path(target))
		node = self.get_or_create(path)
		node.update_from_cache(path, self.cache_path(path))
		node.save()

		args = (node, self.cache_root, callback)
		operation = FileOperation(path, self.swift_connection.update_object, args, pre_execution)
		self.pending_operations.append(operation)
		return 0

	def readlink(self, path):
		return self.get(path).link_source

	### To be implemented...

	def unlink(self, path):
		# TODO: From call traces, it looks like this is what a "delete" turns into. So this is where we should
		# be able to hook in our metadata tagging instead of actual removal (though we could remove from the
		# cache as well)
		def callback(success, error_message):
			# TODO: Once the metadata is set on the object, we could actually remove it from the sqlite
			# local store. This may improve operations that need to weed out deleted files.
			if not success:
				# TODO: log errors
				pass

		def pre_execution():
			node = self.get(path)
			node.deleted_on = deletion_time
			node.save()
			if os.path.exists(self.cache_path(path)):
				os.unlink(self.cache_path(path))

		deletion_time = time.time()
		metadata = { "fs-deleted-on": "%f" % deletion_time }
		args = (path, metadata, callback)
		operation = FileOperation(path, self.swift_connection.set_object_metadata, args, pre_execution)
		self.pending_operations.append(operation)

	def rmdir(self, path):
		# TODO: From call traces, it looks like this is what a "delete" turns into. So this is where we should
		# be able to hook in our metadata tagging instead of actual removal (though we could remove from the
		# cache as well)
		def callback(success, error_message):
			# TODO: Once the metadata is set on the object, we could actually remove it from the sqlite
			# local store. This may improve operations that need to weed out deleted files.
			if not success:
				# TODO: log errors
				pass
		
		def pre_execution():
			pass

		deletion_time = time.time()
		node = self.get(path)
		if len(node.children()) > 0:
			raise FuseOSError(errno.ENOTEMPTY)
		else:
			node.deleted_on = deletion_time
			node.save()
			if os.path.exists(self.cache_path(path)):
				os.rmdir(self.cache_path(path))
			metadata = { "fs-deleted-on": "%f" % deletion_time }
			args = (path, metadata, callback)
			operation = FileOperation(path, self.swift_connection.set_object_metadata, args, pre_execution)
			self.pending_operations.append(operation)


	def rename(self, old, new):
		# Note: This function only gets called when we're moving within the Fuse mount. If
		# external directories are involved, different functions are called.
		old_node = self.get(old)
		def pre_execution():
			pass
		def execute():
			self.create(new, old_node.mode)
			new_node = self.get(new)
			copyfile(self.cache_path(old), self.cache_path(new))
			new_node.dirty = 1
			new_node.save()
			self.unlink(old)
			self.release(new, None)

		if os.path.exists(self.cache_path(old)):
			execute()
		else:
			self.refresh_cache_file(old)
			operation = FileOperation(old, execute, {}, pre_execution)
			self.pending_operations.append(operation)

		return 0

	def statfs(self, path):
		# TODO: 'ls -l' still gives "total 0" at the top on every call, which is probably due
		# to something being incorrectly returned here.
		# return self.object_for_path(path).statfs()

		stv = os.statvfs(path)
		return dict((key, getattr(stv, key)) for key in ('f_bavail', 'f_bfree',
			'f_blocks', 'f_bsize', 'f_favail', 'f_ffree', 'f_files', 'f_flag',
			'f_frsize', 'f_namemax'))

	def truncate(self, path, length, fh=None):
		node = self.get_or_create(path)
		node.update_from_cache(path, self.cache_path(path))

		if fh:
			os.ftruncate(fh, length)
		else:
			with open(self.cache_path(path), 'r+') as f:
				f.truncate(length)

		node.dirty = 1
		node.save()

	### Fuse functions that we might not really need

	def access(self, path, mode):
		# TODO: Is this necessary?
		return 0

	def open(self, path, flags):
		if not os.path.exists(self.cache_path(path)):
			self.refresh_cache_file(path)

		return os.open(self.cache_path(path), flags)

	def create(self, path, mode):
		uid, gid, pid = fuse_get_context()
		path_cache = self.cache_path(path)
		directory = os.path.dirname(path_cache)
		if not os.path.exists(directory):
			os.makedirs(directory)
			os.chown(directory, uid, gid)

		fh = os.open(path_cache, os.O_WRONLY | os.O_CREAT, mode)
		os.chown(path_cache, uid, gid)

		node = self.get(path)
		if not node:
			node = self.get_or_create(path)
			node.update_from_cache(path, self.cache_path(path))

		node.save()

		return fh

	def mknod(self, path, mode, dev):
		# TODO: Is this necessary?
		return 0

	def flush(self, path, fh):
		# TODO: Do we need to implement this? We can't use the python os.fsync method unless we actually
		# have a valid file handle, and we don't really keep track of them or use them in our implementation.
		# return os.fsync(fh)
		if fh:
			return os.fsync(fh)
		else:
			return 0

	def fsync(self, path, datasync, fh):
		# TODO: Do we need to implement this? We can't use the python os.fsync method unless we actually
		# have a valid file handle, and we don't really keep track of them or use them in our implementation.
		# return os.fsync(fh)
		if fh:
			return os.fsync(fh)
		else:
			return 0

	### Needs more research / implementation thought

	def utimens(self, path, times=None):
		return 0

	def link(self, target, source):
		# TODO: We *might* be able to implement this reliably. We'd have to check first that the hard link was being
		# done where target and source were both under the cache_root (meaning they are both under the same swift backed
		# "filesystem"). Then we'd add an extra field of metadata, much like that for a symbolic link (perhaps even use
		# the same metadata field: "fs-link-source") and when a file like this was created, we'd set the metadata value
		# instead of uploading contents. We'd also have to be aware of this during the re-creation of the filesystem upon
		# mounting. Of course, the database would also have to be updated in the same way. We may need to add another
		# attribute type to indicate that the object is actually a hard link, as opposed to a regular file (as the mode
		# info won't do this in the same way it does for directories and symbolic links). But we may also be able to use
		# whether or not the "link-source" is set and whether the file looks like a regular file otherwise in order to
		# differentiate.
		#
		# The most important thing would be to follow the principle of a 1-to-1 mapping between filesystem objects and
		# swift objects. If everything on the filesystem is represented by an object in swift, then we can easily re-create
		# it without a lot of magic.
		return 0

	def getxattr(self, path, name, position=0):
		# TODO: Can/should we provide something for this?
		return ""

	def listxattr(self, path):
		return None




	####### Helper Functions #######

	def get_or_create(self, path):
		node = self.get(path, include_deleted=True)
		if node:
			node.undelete()
			return node
		else:
			return FSNode()

	def get(self, path, include_deleted=False):
		fsnode = FSNode.get_by_path(path)
		# Don't return the node if a soft delete has been performed on it
		if fsnode and (include_deleted or not fsnode.is_deleted(self.snapshot_timestamp())):
			return fsnode
		else:
			return None

	def cache_path(self, path):
		return os.path.join(self.cache_root, path.lstrip("/"))

	def refresh_cache_file(self, path):
		def callback(success, error_message):
			# TODO: is there any circumstance that we don't want to clear the downloading field
			#       in the node?
			node = self.get(path)
			node.downloading = None
			node.save()
			if not success:
				# TODO: need to be logging failures and in this case probably retry the download
				pass

		# Make sure the path exists to grab the file to.
		# TODO: make sure to set mode/gid/uid and various times according to what's in the metadata db
		# as all of these folders should actually have objects and thus database items for them.
		cache_folder_path = os.path.dirname(self.cache_path(path))
		if not os.path.exists(cache_folder_path):
			os.makedirs(cache_folder_path)

		# TODO: start a process to grab this object in the background and return the requested
		# data from this function as soon as it's available

		# Make sure the path exists to grab the file to.
		# TODO: make sure to set mode/gid/uid and various times according to what's in the metadata db
		# as all of these folders should actually have objects and thus database items for them.
		cache_folder_path = os.path.dirname(self.cache_path(path))
		if not os.path.exists(cache_folder_path):
			os.makedirs(cache_folder_path)

		# First we make sure that the file exists so other methods can open it and query the size, ect
		uid, gid, pid = fuse_get_context()
		open(self.cache_path(path), 'a').close()
		os.chown(self.cache_path(path), uid, gid)
		
		# Now we mark the node as download in progress
		node = self.get(path)
		node.downloading = time.time()
		node.save()
		self.swift_connection.download_object(path.lstrip("/"), self.cache_path(path), callback)

	def refresh_from_object_store(self):
		FSNode._fsdata = {}

		# Add the root node
		node = FSNode()
		node.update_from_cache("/", self.cache_path("/"))
		node.save()

		for obj in self.swift_connection.get_objects("/"):
			node = FSNode()
			node.update_from_swift(obj)
			if not node.is_deleted(self.snapshot_timestamp()):
				node.save()


	def snapshot_timestamp(self):
		if "snapshot_time" in self.config:
			return time.mktime(dateutil.parser.parse(self.config["snapshot_time"]).timetuple())
		else:
			return None

	def __call__(self, op, path, *args):
		# print "calling %s (path: %s | args: %s)" % (op, path, args)
		retval = super(FileSystem, self).__call__(op, path, *args)

		# if op != "get":
		# 	print "retval: %s from %s (path: %s | args: %s)" % (retval, op, path, args)

		return retval
	
	def _job_executor_thread_main(self):
		# TODO: limit the number of attempts
		while True:
			print "job deque: " + str(len(self.pending_operations))
			try:
				op = self.pending_operations.popleft()
			except IndexError, e:
				time.sleep(1)
				continue
			node = self.get(op.path)
			if node is None:
				time.sleep(1)
				op.attempt += 1
				print "node does not exist: %s" % op.path
				continue
			if node.uploading is not None or node.downloading is not None:
				print "upload or download in progress, %s-%s-|" % (str(node.uploading), str(node.downloading))
				self.pending_operations.appendleft(op)
				continue
			op.pre_execution()
			op.operation(*op.operation_args)

class FileOperation:
	"""
	Parameters
		node: should be the file node object
		operation: this is the function to be executed once there is no active job running on the node
		operation_args: a tuple of arguments to be passed to the operation function
		pre_execution: a function to be executed before the operation. This is where things like 'uploading' should be set
	"""
	def __init__(self, path, operation, operation_args, pre_execution):
		self.path = path
		self.operation = operation
		self.operation_args = operation_args
		self.pre_execution = pre_execution
		self.attempt = 0



import metadata

import os, time, dateutil.parser, errno
from stat import S_IFDIR, S_IFLNK, S_IFREG

from fuse import FuseOSError, Operations, LoggingMixIn

from swift_source import SwiftSource

from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine

from threading import Lock

Session = sessionmaker()
Base = declarative_base()

from fsnode import FSNode

class FileSystem(LoggingMixIn, Operations):
	def __init__(self, config):
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

		# TODO: write the database somewhere other than the current directory
		self.engine = create_engine('sqlite:///metadata.db')
		Session.configure(bind=self.engine)
		self.refresh_from_object_store()

	####### FUSE Functions #######

	### Fuse functions that need to work with the object store

	def chmod(self, path, mode):
		session = Session()
		node = self.get(path, session)
		node.mode = mode
		session.add(node)
		session.commit()
		# TODO: This has the potential to be quite slow. We may want to send the
		# operation off to a background process
		obj = self.swift_connection.get_object(path)
		obj.set_metadata({ "fs-mode": "%i" % mode })
		return 0

	def chown(self, path, uid, gid):
		session = Session()
		node = self.get(path, session)
		node.uid = uid
		node.gid = gid
		session.add(node)
		session.commit()
		# TODO: This has the potential to be quite slow. We may want to send the
		# operation off to a background process
		obj = self.swift_connection.get_object(path)
		obj.set_metadata({ "fs-uid": "%i" % uid, "fs-gid": "%i" % gid })
		return 0

	def getattr(self, path, fh=None):
		if path == "/":
			st = os.lstat(self.cache_root)
			return dict((key, getattr(st, key)) for key in ('st_atime', 'st_ctime',
				'st_gid', 'st_mode', 'st_mtime', 'st_nlink', 'st_size', 'st_uid'))
		else:
			node = self.get(path, Session())

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
		node = self.create_fsnode(path, Session())
		self.swift_connection.update_object(node, self.cache_root)

		return 0

	def read(self, path, size, offset, fh):
		if fh:
			with self.rwlock:
				os.lseek(fh, offset, 0)
				return os.read(fh, size)
		else:
			raise FuseOSError(errno.ENOENT)

	def readdir(self, path, fh):
		session = Session()
		dir_files = [fsnode for fsnode in session.query(FSNode).filter(FSNode.folder==path.lstrip("/"))]
		current_time = time.time()
		valid_files = [fsnode for fsnode in dir_files if not fsnode.deleted_on or fsnode.deleted_on > current_time]
		return [fsnode.name for fsnode in valid_files]

	def write(self, path, data, offset, fh):
		# Make sure the path exists to grab the file to.
		# TODO: make sure to set mode/gid/uid and various times according to what's in the metadata db
		# as all of these folders should actually have objects and thus database items for them.
		cache_folder_path = os.path.dirname(self.cache_path(path))
		if not os.path.exists(cache_folder_path):
			os.makedirs(cache_folder_path)

		session = Session()
		node = self.get(path, session)

		os.lseek(fh, offset, 0)
		retval = os.write(fh, data)

		node.dirty = 1
		session.add(node)
		session.commit()

		# Strange things happen if you don't return the number of bytes written from this function call.
		return retval

	def release(self, path, fh):
		if fh:
			os.close(fh)

		session = Session()
		node = self.get(path, session)
		if node and node.dirty == 1:
			self.update_fsnode(path, node)
			self.swift_connection.update_object(node, self.cache_root)
			node.dirty = 0
			session.add(node)
			session.commit()
		return 0

	def symlink(self, target, source):
		# TODO: Handle existing symbolic link
		os.symlink(source, self.cache_path(target))
		node = self.create_fsnode(target, Session())
		self.swift_connection.update_object(node, self.cache_root)

		return 0

	def readlink(self, path):
		return self.get(path, Session()).link_source

	### To be implemented...

	def unlink(self, path):
		# TODO: From call traces, it looks like this is what a "delete" turns into. So this is where we should
		# be able to hook in our metadata tagging instead of actual removal (though we could remove from the
		# cache as well)
		deletion_time = time.time()

		session = Session()
		node = self.get(path, session)
		node.deleted_on = deletion_time
		session.add(node)
		session.commit()

		obj = self.swift_connection.get_object(path)
		if obj:
			obj.set_metadata({ "fs-deleted-on": "%f" % deletion_time })
			# TODO: Once the metadata is set on the object, we could actually remove it from the sqlite
			# local store. This may improve operations that need to weed out deleted files.

		if os.path.exists(self.cache_path(path)):
			os.unlink(self.cache_path(path))

	def rmdir(self, path):
		# return an appropriate error code if the directory has files in it
		return 0

	def rename(self, old, new):
		# Note: This function only gets called when we're moving within the Fuse mount. If
		# external directories are involved, different functions are called.
		if os.path.exists(self.cache_path(old)):
			if os.path.exists(self.cache_path(new)):
				os.rename(self.cache_path(old), self.cache_path(new))
			else:
				os.unlink(self.cache_path(old))

		session = Session()
		node = self.get(old, session)

		path_data = new.lstrip("/").rsplit('/', 1)
		if len(path_data) == 1:
			file_folder = ""
			file_name = path_data[0]
		else:
			file_folder = path_data[0]
			file_name = path_data[1]

		node.name = file_name
		node.folder = file_folder
		node.path = new.lstrip("/")
		session.add(node)
		session.commit()

		self.swift_connection.move_object(old, new)

		return 0

	def statfs(self, path):
		return self.object_for_path(path).statfs()

	def truncate(self, path, length, fh=None):
		session = Session()
		node = self.get(path, session)

		if fh:
			os.ftruncate(fh, length)
		else:
			with open(self.cache_path(path), 'r+') as f:
				f.truncate(length)

		node.dirty = 1
		session.add(node)
		session.commit()

	### Fuse functions that we might not really need

	def access(self, path, mode):
		# TODO: Is this necessary?
		return 0

	def open(self, path, flags):
		if not os.path.exists(self.cache_path(path)):
			self.refresh_cache_file(path)

		return os.open(self.cache_path(path), flags)

	def create(self, path, mode):
		fh = os.open(self.cache_path(path), os.O_WRONLY | os.O_CREAT, mode)

		session = Session()
		node = self.get(path, session)
		if not node:
			node = self.create_fsnode(path, session)

		session.add(node)
		session.commit()

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

	def get(self, path, session):
		return session.query(FSNode).filter(FSNode.path==path.lstrip("/")).first()

	def cache_path(self, path):
		return os.path.join(self.cache_root, path.lstrip("/"))

	def refresh_cache_file(self, path):
		# Make sure the path exists to grab the file to.
		# TODO: make sure to set mode/gid/uid and various times according to what's in the metadata db
		# as all of these folders should actually have objects and thus database items for them.
		cache_folder_path = os.path.dirname(self.cache_path(path))
		if not os.path.exists(cache_folder_path):
			os.makedirs(cache_folder_path)

		# TODO: start a process to grab this object in the background and return the requested 
		# data from this function as soon as it's available
		obj = self.swift_connection.get_object(path)

		# Make sure the path exists to grab the file to.
		# TODO: make sure to set mode/gid/uid and various times according to what's in the metadata db
		# as all of these folders should actually have objects and thus database items for them.
		cache_folder_path = os.path.dirname(self.cache_path(path))
		if not os.path.exists(cache_folder_path):
			os.makedirs(cache_folder_path)
		cache_file = open(self.cache_path(path), 'w')
		cache_file.write(obj.fetch()) # python will convert \n to os.linesep
		cache_file.close()

	def refresh_from_object_store(self):
		# Drop and recreate the database from scratch
		Base.metadata.drop_all(self.engine)
		Base.metadata.create_all(self.engine)

		session = Session()
		for obj in self.swift_connection.get_objects("/"):
			obj_metadata = obj.get_metadata()

			include_fsnode = True
			if 'x-object-meta-fs-deleted-on' in obj_metadata:
				snapshot_timestamp = self.snapshot_timestamp()
				if not snapshot_timestamp or float(obj_metadata['x-object-meta-fs-deleted-on']) <= snapshot_timestamp:
					include_fsnode = False

			# If mounting with a snapshot timestamp, check whether 
			if include_fsnode:
				# split the file name out from its parent directory
				path_data = obj.name.rsplit('/', 1)
				if len(path_data) == 1:
					file_folder = ""
					file_name = path_data[0]
				else:
					file_folder = path_data[0]
					file_name = path_data[1]

				node = FSNode(
					path=obj.name,
					name=file_name,
					folder=file_folder,
					mode=obj_metadata['x-object-meta-fs-mode'],
					uid=obj_metadata['x-object-meta-fs-uid'],
					gid=obj_metadata['x-object-meta-fs-gid'],
					mtime=obj_metadata['x-object-meta-fs-mtime'],
					atime=obj_metadata['x-object-meta-fs-atime'],
					ctime=obj_metadata['x-object-meta-fs-ctime'],
					nlink=obj_metadata['x-object-meta-fs-nlink'],
					size=obj_metadata['x-object-meta-fs-size'],
					dirty=0)

				if 'x-object-meta-fs-link-source' in obj_metadata:
					node.link_source = obj_metadata['x-object-meta-fs-link-source']

				session.add(node)

		session.commit()

	def create_fsnode(self, path, session):
		# split the file name out from its parent directory
		path_data = path.lstrip("/").rsplit('/', 1)
		if len(path_data) == 1:
			file_folder = ""
			file_name = path_data[0]
		else:
			file_folder = path_data[0]
			file_name = path_data[1]

		cached_st = os.lstat(self.cache_path(path))
		cached_attr = dict((key, getattr(cached_st, key)) for key in ('st_atime', 'st_ctime',
			'st_gid', 'st_mode', 'st_mtime', 'st_nlink', 'st_size', 'st_uid'))

		link_source = None
		if os.path.islink(self.cache_path(path)):
			link_source = os.readlink(self.cache_path(path))

		node = FSNode()
		self.update_fsnode(path, node)
		node.dirty = 0

		session.add(node)
		session.commit()

		return node

	def update_fsnode(self, path, node):
		# split the file name out from its parent directory
		path_data = path.lstrip("/").rsplit('/', 1)
		if len(path_data) == 1:
			file_folder = ""
			file_name = path_data[0]
		else:
			file_folder = path_data[0]
			file_name = path_data[1]

		cached_st = os.lstat(self.cache_path(path))
		cached_attr = dict((key, getattr(cached_st, key)) for key in ('st_atime', 'st_ctime',
			'st_gid', 'st_mode', 'st_mtime', 'st_nlink', 'st_size', 'st_uid'))

		link_source = None
		if os.path.islink(self.cache_path(path)):
			link_source = os.readlink(self.cache_path(path))

		node.path=path.lstrip("/")
		node.name=file_name
		node.folder=file_folder
		node.mode=int(cached_attr['st_mode'])
		node.uid=int(cached_attr['st_uid'])
		node.gid=int(cached_attr['st_gid'])
		node.mtime=float(cached_attr['st_mtime'])
		node.atime=float(cached_attr['st_atime'])
		node.ctime=float(cached_attr['st_ctime'])
		node.nlink=int(cached_attr['st_nlink'])
		node.size=int(cached_attr['st_size'])
		node.link_source=link_source

		return node

	def snapshot_timestamp(self):
		if "snapshot_time" in self.config:
			return time.mktime(dateutil.parser.parse(self.config["snapshot_time"]).timetuple())
		else:
			return None

	def __call__(self, op, path, *args):
		print "calling %s (path: %s | args: %s)" % (op, path, args)
		retval = super(FileSystem, self).__call__(op, path, *args)

		if op != "get":
			print "retval: %s from %s (path: %s | args: %s)" % (retval, op, path, args)

		return retval
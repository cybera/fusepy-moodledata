import metadata

import os
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
		self.swift_connection.create_object(node, self.cache_root)

		return 0

	def read(self, path, size, offset, fh):
		if not os.path.exists(self.cache_path(path)):
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

		# TODO: Do we need to grab a lock before the read, as in the loopback example?
		cache_file = open(self.cache_path(path), 'r')
		cache_file.seek(offset)
		return cache_file.read(size)

	def readdir(self, path, fh):
		session = Session()
		return [row.name for row in session.query(FSNode).filter(FSNode.folder==path.lstrip("/"))]

	def write(self, path, data, offset, fh):
		# Make sure the path exists to grab the file to.
		# TODO: make sure to set mode/gid/uid and various times according to what's in the metadata db
		# as all of these folders should actually have objects and thus database items for them.
		cache_folder_path = os.path.dirname(self.cache_path(path))
		if not os.path.exists(cache_folder_path):
			os.makedirs(cache_folder_path)

		with self.rwlock:
			# TODO: Verify that we don't need to be taking special care to go to a specific offset.
			# We're assuming here that any write with an offset value is simply trying to append to
			# the file.
			if offset == 0:
				with open(self.cache_path(path), 'w') as cache_file:
					cache_file.write(data)
			else:
				with open(self.cache_path(path), 'a') as cache_file:
					cache_file.write(data)

		session = Session()
		node = self.get(path, session)
		if not node:
			node = self.create_fsnode(path, session)

		node.dirty = 1
		session.add(node)
		session.commit()

		return len(data)

	def release(self, path, fh):
		session = Session()
		node = self.get(path, session)
		if node and node.dirty == 1:
			self.swift_connection.create_object(node, self.cache_root)
			node.dirty = 0
			session.add(node)
			session.commit()
		return 0

	def rename(self, old, new):
		return self.file_system.rename(old, new)

	def rmdir(self, path):
		return self.object_for_path(path).rmdir()

	def statfs(self, path):
		return self.object_for_path(path).statfs()

	### Fuse functions that can just work on the cache

	def access(self, path, mode):
		# TODO: Is this necessary?
		if not os.access(self.cache_path(path), mode):
			raise FuseOSError(EACCES)

	def open(self, path, flags):
		# TODO: Do we really need to do anything here?
		# Note that if we simply call os.open on the cache path file, we will not "find" a file
		# that isn't already cached. So perhaps we should be doing the swift retrieval here? Or
		# just access the sqlite database and set some sort of "opened" flag?
		# return os.open(self.cache_path(path), flags)
		return 0

	def create(self, path, mode):
		# TODO: Is this necessary?
		return os.open(self.cache_path(path), os.O_WRONLY | os.O_CREAT, mode)

	def mknod(self, path, mode, dev):
		# TODO: Is this necessary?
		return os.mknod(self.cache_path(path), mode, dev)

	def flush(self, path, fh):
		# TODO: Do we need to implement this? We can't use the python os.fsync method unless we actually
		# have a valid file handle, and we don't really keep track of them or use them in our implementation.
		# return os.fsync(fh)
		return 0

	def fsync(self, path, datasync, fh):
		# TODO: Do we need to implement this? We can't use the python os.fsync method unless we actually
		# have a valid file handle, and we don't really keep track of them or use them in our implementation.
		# return os.fsync(fh)
		return 0

	### Needs more research / implementation thought

	def symlink(self, target, source):
		# TODO: Just like a directory, an object should be able to be created for the symlink (with empty data)
		# and the actual fact that it's a symlink should be able to be determined from the mode of the cached
		# file. So, just like a directory, make the equivalent symlink in the cache directory and then grab its
		# info for the object metadata
		return 0

	def truncate(self, path, length, fh=None):
		# TODO: In the loopback filesystem example, it looks like this is just a variant of read?
		return 0

	def unlink(self, path):
		# TODO: From call traces, it looks like this is what a "delete" turns into. So this is where we should
		# be able to hook in our metadata tagging instead of actual removal (though we could remove from the
		# cache as well)
		return 0

	def readlink(self, path):
		return 0

	def utimens(self, path, times=None):
		return 0

	def link(self, target, source):
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

	def refresh_from_object_store(self):
		# Drop and recreate the database from scratch
		Base.metadata.drop_all(self.engine)
		Base.metadata.create_all(self.engine)

		session = Session()
		for obj in self.swift_connection.get_objects("/"):
			obj_metadata = obj.get_metadata()

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

		node = FSNode(
			path=path.lstrip("/"),
			name=file_name,
			folder=file_folder,
			mode=int(cached_attr['st_mode']),
			uid=int(cached_attr['st_uid']),
			gid=int(cached_attr['st_gid']),
			mtime=float(cached_attr['st_mtime']),
			atime=float(cached_attr['st_atime']),
			ctime=float(cached_attr['st_ctime']),
			nlink=int(cached_attr['st_nlink']),
			size=int(cached_attr['st_size']),
			dirty=0)
		session.add(node)
		session.commit()

		return node

	def __call__(self, op, path, *args):
		retval = super(FileSystem, self).__call__(op, path, *args)

		if op != "get":
			print "called %s (path: %s | args: %s)" % (op, path, args)

		return retval
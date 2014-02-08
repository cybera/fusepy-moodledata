import metadata

import os

from fuse import FuseOSError, Operations, LoggingMixIn

from swift_source import SwiftSource

from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine

Session = sessionmaker()
Base = declarative_base()

from fsnode import FSNode

class FileSystem(LoggingMixIn, Operations):
	def __init__(self, config):
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
				size=obj_metadata['x-object-meta-fs-size'])
			session.add(node)

		session.commit()

	def get(self, path, session):
		return session.query(FSNode).filter(FSNode.path==path.lstrip("/")).first()


	def access(self, path, mode):
		print "calling access"
		return self.file_system.access(path, mode)

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

	def create(self, path, mode):
		return self.file_system.create(path, mode)

	def flush(self, path, fh):
		# TODO: This may be the spot to write modified files back to the object store
		return 0

	def fsync(self, path, datasync, fh):
		return self.object_for_path(path).fsync(datasync, fh)

	def getattr(self, path, fh=None):
		if path == "/":
			st = os.lstat(self.cache_root)
			return dict((key, getattr(st, key)) for key in ('st_atime', 'st_ctime',
				'st_gid', 'st_mode', 'st_mtime', 'st_nlink', 'st_size', 'st_uid'))
		else:
			return self.get(path, Session()).attr()

	def getxattr(self, path, name, position=0):
		return ""
	
	def link(self, target, source):
		return self.file_system.link(target, source)

	def listxattr(self, path):
		return None

	def mkdir(self, path, mode):
		return self.file_system.mkdir(path, mode)

	def mknod(self, path, mode, dev):
		return self.file_system.mknod(path, mode, dev)

	def open(self, path, flags):
		# TODO: Do we really need to do anything here?
		return 0

	def read(self, path, size, offset, fh):
		if not os.path.exists(self.cache_path(path)):
			# TODO: start a process to grab this object in the background and return the requested 
			# data from this function as soon as it's available
			obj = self.swift_connection.get_object(path)
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

	def readlink(self, path):
		return self.object_for_path(path).readlink()

	def release(self, path, fh):
		# TODO: This may be the spot to write modified files back to the object store
		return 0

	def rename(self, old, new):
		return self.file_system.rename(old, new)

	def rmdir(self, path):
		return self.object_for_path(path).rmdir()

	def statfs(self, path):
		return self.object_for_path(path).statfs()

	def symlink(self, target, source):
		return self.file_system.symlink(target, source)

	def truncate(self, path, length, fh=None):
		return self.object_for_path(path).truncate(length, fh)

	def unlink(self, path):
		return self.file_system.unlink(path)

	def utimens(self, path, times=None):
		return self.object_for_path(path).utimens(times)

	def write(self, path, data, offset, fh):
		return self.object_for_path(path).write(data, offset, fh)


	def cache_path(self, path):
		return os.path.join(self.cache_root, path.lstrip("/"))

	# def mknod(self, path, mode, dev):
	# 	return os.mknod(self.cache_path(path), mode, dev)

	# def mkdir(self, path, mode):
	# 	if path not in self.access_wrappers:
	# 		self.access_wrappers[path] = Directory(path, self)
	# 	return os.mkdir(self.cache_path(path), mode)

	# def readdir(self, path, fh):
	# 	return ['.', '..'] + [row[0] for row in self.db.select("SELECT name FROM nodes WHERE path = ?", path.lstrip("/"))]

	# def link(self, target, source):
	# 	return os.link(source, self.cache_path(target))

	# def unlink(self, path):
	# 	return os.unlink(self.cache_path(path))

	# def symlink(self, target, source):
	# 	return os.symlink(source, self.cache_path(target))

	# def access(self, path, mode):
	# 	if not os.access(self.cache_path(path), mode):
	# 		raise FuseOSError(EACCES)

	# def rename(self, old, new):
	# 	self.access_wrappers[old] = None
	# 	return os.rename(self.cache_path(old), self.cache_path(new))

	# def create(self, path, mode):
	# 	if path not in self.access_wrappers:
	# 		self.access_wrappers[path] = File(path, self)
	# 	return os.open(self.cache_path(path), os.O_WRONLY | os.O_CREAT, mode)

	def __call__(self, op, path, *args):
		retval = super(FileSystem, self).__call__(op, path, *args)

		if op != "get":
			print "called %s (path: %s | args: %s)" % (op, path, args)

		return retval
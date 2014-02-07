import metadata

import os
# import metadata
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine

Session = sessionmaker()
Base = declarative_base()

from fsnode import File, Directory, FSNode, Node

class FileSystem:
	def __init__(self, cache_root, swift_connection):
		self.access_wrappers = {}
		self.cache_root = cache_root
		self.swift_connection = swift_connection
		self.db = metadata.Database('metadata.db')
		self.engine = create_engine('sqlite:///metadata.db')
		Session.configure(bind=self.engine)
		self.__refresh_from_swift__()
		

	def __refresh_from_swift__(self):
		self.db.update('''DROP TABLE IF EXISTS nodes''')
		Base.metadata.create_all(self.engine)
		# self.db.update('''CREATE TABLE nodes (
		# 		id 		INTEGER PRIMARY KEY AUTOINCREMENT,
		# 		name 	TEXT NOT NULL,
		# 		path 	TEXT NOT NULL,
		# 		mode 	INT NOT NULL,
		# 		uid 	INT NOT NULL,
		# 		gid 	INT NOT NULL,
		# 		mtime	REAL NOT NULL,
	 #        	atime	REAL NOT NULL,
	 #        	ctime	REAL NOT NULL,
	 #        	size 	INT NOT NULL DEFAULT 0
		# 	)''')
		session = Session()
		for obj in self.swift_connection.get_objects("/"):
			obj_metadata = obj.get_metadata()
			
			# split the file name out from its parent directory
			path_data = obj.name.rsplit('/', 1)
			if len(path_data) == 1:
				file_path = ""
				file_name = path_data[0]
			else:
				file_path = path_data[0]
				file_name = path_data[1]				

			node = Node(
				name=file_name, 
				path=file_path,
				mode=obj_metadata['x-object-meta-fs-mode'],
				uid=obj_metadata['x-object-meta-fs-uid'], 
				gid=obj_metadata['x-object-meta-fs-gid'], 
				mtime=obj_metadata['x-object-meta-fs-mtime'], 
				atime=obj_metadata['x-object-meta-fs-atime'], 
				ctime=obj_metadata['x-object-meta-fs-ctime'], 
				size=obj_metadata['x-object-meta-fs-size'])
			session.add(node)

			# self.db.update("INSERT INTO nodes (name, path, mode, uid, gid, mtime, atime, ctime, size) VALUES (?, ?,?,?,?,?,?,?,?)", 
			# 	file_name,
			# 	file_path,
			# 	obj_metadata['x-object-meta-fs-mode'], 
			# 	obj_metadata['x-object-meta-fs-uid'], 
			# 	obj_metadata['x-object-meta-fs-gid'], 
			# 	obj_metadata['x-object-meta-fs-mtime'], 
			# 	obj_metadata['x-object-meta-fs-atime'], 
			# 	obj_metadata['x-object-meta-fs-ctime'], 
			# 	obj_metadata['x-object-meta-fs-size'])
		session.commit()

	def get(self, path):
		if metadata.exists(path):
			if path not in self.access_wrappers:
				if metadata.is_directory(path):
					self.access_wrappers[path] = Directory(path, self)
				else:
					self.access_wrappers[path] = File(path, self)

			return self.access_wrappers[path]
		else:
			# Don't try to store this. We know it will soon change or be irrelevant
			return FSNode(path, self)


	def cache_path(self, path):
		return os.path.join(self.cache_root, path.lstrip("/"))

	def mknod(self, path, mode, dev):
		return os.mknod(self.cache_path(path), mode, dev)

	def mkdir(self, path, mode):
		if path not in self.access_wrappers:
			self.access_wrappers[path] = Directory(path, self)
		return os.mkdir(self.cache_path(path), mode)

	def readdir(self, path, fh):
		return ['.', '..'] + [row[0] for row in self.db.select("SELECT name FROM nodes WHERE path = ?", path.lstrip("/"))]

	def link(self, target, source):
		return os.link(source, self.cache_path(target))

	def unlink(self, path):
		return os.unlink(self.cache_path(path))

	def symlink(self, target, source):
		return os.symlink(source, self.cache_path(target))

	def access(self, path, mode):
		if not os.access(self.cache_path(path), mode):
			raise FuseOSError(EACCES)

	def rename(self, old, new):
		self.access_wrappers[old] = None
		return os.rename(self.cache_path(old), self.cache_path(new))

	def create(self, path, mode):
		if path not in self.access_wrappers:
			self.access_wrappers[path] = File(path, self)
		return os.open(self.cache_path(path), os.O_WRONLY | os.O_CREAT, mode)

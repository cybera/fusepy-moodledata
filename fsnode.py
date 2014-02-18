import os, stat, time

from threading import Lock
from sqlalchemy import Column, Integer, String, INT, REAL
from file_system import Session
from file_system import Base

class FSNode(Base):
	__tablename__ = "nodes"
	id = Column(Integer, primary_key=True)
	path = Column(String)
	name = Column(String)
	folder = Column(String)
	mode = Column(INT)
	uid = Column(INT)
	gid = Column(INT)
	mtime = Column(REAL)
	atime = Column(REAL)
	ctime = Column(REAL)
	nlink = Column(INT)
	size = Column(INT)
	dirty = Column(INT)
	link_source = Column(String)
	deleted_on = Column(REAL)

	def attr(self):
		return {
			'st_atime': self.atime,
			'st_ctime': self.ctime,
			'st_gid': self.gid, 
			'st_mode': self.mode, 
			'st_mtime': self.mtime,
			'st_nlink': self.nlink, 
			'st_size': self.size,
			'st_uid': self.uid
		}

	def is_directory(self):
		return stat.S_ISDIR(self.mode)

	def is_file(self):
		return stat.S_ISREG(self.mode)

	def is_symbolic_link(self):
		return stat.S_ISLNK(self.mode)

	def children(self, session):
		dir_files = [fsnode for fsnode in session.query(FSNode).filter(FSNode.folder==self.path.lstrip("/")).filter(FSNode.path!="")]
		current_time = time.time()
		return [fsnode for fsnode in dir_files if not fsnode.deleted_on or fsnode.deleted_on > current_time]

	def update_from_cache(self, path, file_system):
		# split the file name out from its parent directory
		path_data = path.lstrip("/").rsplit('/', 1)
		if len(path_data) == 1:
			file_folder = ""
			file_name = path_data[0]
		else:
			file_folder = path_data[0]
			file_name = path_data[1]

		cached_st = os.lstat(file_system.cache_path(path))
		cached_attr = dict((key, getattr(cached_st, key)) for key in ('st_atime', 'st_ctime',
			'st_gid', 'st_mode', 'st_mtime', 'st_nlink', 'st_size', 'st_uid'))

		link_source = None
		if os.path.islink(file_system.cache_path(path)):
			link_source = os.readlink(file_system.cache_path(path))

		self.path=path.lstrip("/")
		self.name=file_name
		self.folder=file_folder
		self.mode=int(cached_attr['st_mode'])
		self.uid=int(cached_attr['st_uid'])
		self.gid=int(cached_attr['st_gid'])
		self.mtime=float(cached_attr['st_mtime'])
		self.atime=float(cached_attr['st_atime'])
		self.ctime=float(cached_attr['st_ctime'])
		self.nlink=int(cached_attr['st_nlink'])
		self.size=int(cached_attr['st_size'])
		self.link_source=link_source


	def update_from_swift(self, swift_obj, file_system):
		obj_metadata = swift_obj.get_metadata()

		include_fsnode = True
		if 'x-object-meta-fs-deleted-on' in obj_metadata:
			snapshot_timestamp = file_system.snapshot_timestamp()
			if not snapshot_timestamp or float(obj_metadata['x-object-meta-fs-deleted-on']) <= snapshot_timestamp:
				include_fsnode = False

		# If mounting with a snapshot timestamp, check whether 
		if include_fsnode:
			# split the file name out from its parent directory
			path_data = swift_obj.name.rsplit('/', 1)
			if len(path_data) == 1:
				file_folder = ""
				file_name = path_data[0]
			else:
				file_folder = path_data[0]
				file_name = path_data[1]

			self.path=swift_obj.name
			self.name=file_name
			self.folder=file_folder
			self.mode=obj_metadata['x-object-meta-fs-mode']
			self.uid=obj_metadata['x-object-meta-fs-uid']
			self.gid=obj_metadata['x-object-meta-fs-gid']
			self.mtime=obj_metadata['x-object-meta-fs-mtime']
			self.atime=obj_metadata['x-object-meta-fs-atime']
			self.ctime=obj_metadata['x-object-meta-fs-ctime']
			self.nlink=obj_metadata['x-object-meta-fs-nlink']
			self.size=obj_metadata['x-object-meta-fs-size']
			self.dirty=0

			if 'x-object-meta-fs-link-source' in obj_metadata:
				self.link_source = obj_metadata['x-object-meta-fs-link-source']

	# def __init__(self, path, file_system):
	# 	self.path = path
	# 	self.file_system = file_system
	# 	self.rwlock = Lock()

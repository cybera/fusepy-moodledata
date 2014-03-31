import os, stat, time

from threading import Lock
from peewee import *

import file_system

class BaseModel(Model):
	"""A base model that will use our Sqlite database"""
	class Meta:
		database = file_system.global_db

class FSNode(BaseModel):
	class Meta:
		db_table = "nodes"

	id = PrimaryKeyField()
	path = CharField()
	name = CharField()
	folder = CharField()
	mode = IntegerField()
	uid = IntegerField()
	gid = IntegerField()
	mtime = DoubleField()
	atime = DoubleField()
	ctime = DoubleField()
	nlink = IntegerField()
	size = IntegerField()
	dirty = IntegerField(null=True)
	link_source = CharField(null=True)
	deleted_on = DoubleField(null=True)

	# if downloading or uploading fields are not null they should be set to the timestamp
	# of when the download/upload was started. From this and the file size we can calculate
	# reasonable timeouts.
	downloading = IntegerField(null=True)
	uploading = IntegerField(null=True)

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

	def is_deleted(self, as_of_timestamp=None):
		if as_of_timestamp:
			return self.deleted_on is not None and self.deleted_on <= as_of_timestamp
		else:
			return self.deleted_on is not None

	def delete(self):
		self.deleted_on = time.time()

	def undelete(self):
		self.deleted_on = None

	def children(self):
		dir_files = [fsnode for fsnode in FSNode.select().where(FSNode.folder==self.path.lstrip("/")).where(FSNode.path!="")]
		# dir_files = [fsnode for fsnode in session.query(FSNode).filter(FSNode.folder==self.path.lstrip("/")).filter(FSNode.path!="")]
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


	def update_from_swift(self, swift_obj):
		obj_metadata = swift_obj.get_metadata()

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

		if 'x-object-meta-fs-deleted-on' in obj_metadata:
			self.deleted_on=obj_metadata['x-object-meta-fs-deleted-on']

		if 'x-object-meta-fs-link-source' in obj_metadata:
			self.link_source = obj_metadata['x-object-meta-fs-link-source']

	# def __init__(self, path, file_system):
	# 	self.path = path
	# 	self.file_system = file_system
	# 	self.rwlock = Lock()

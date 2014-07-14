import os, stat, time

from threading import Lock

import file_system
from swift_source import SwiftSource

class FSNode:
	"""
	FSNode represents either a file, directory. This class will work as is when using in-memory storage for file metadata.
	To use a different data backend you will likely need to create a subclass and override some methods.

	Public Attributes:
		path         string   The full path of the object from the root of the file system
		name         string   File name
		folder       string   The folder path (eg. this/is/my/subfolder)
		link_source  string   Can be None
		mode         integer  (non-negative)
		uid          integer  (non-negative)
		gid          integer  (non-negative)
		nlink        integer  (non-negative)
		size         integer  (non-negative)
		mtime        double   (timestamp)
		atime        double   (timestamp)
		ctime        double   (timestamp)
		deleted_on   double   (timestamp)   Can be None
		downloading  double   (timestamp)   Can be None
		uploading    double   (timestamp)   Can be None
		dirty        boolean  Can be None
	"""
	# the 'folder' is the key to the root and the value is anonther hash with directory contents where each
	# value is a FSNode object
	_fsdata = {}
	_swift_connection = None

	def __init__(self, deleted_on=None, downloading=None, uploading=None, dirty=None, link_source=None):
		self.link_source = link_source
		self.deleted_on = deleted_on
		self.dirty = dirty
		self.downloading = downloading
		self.uploading = uploading

	def attr(self):
		result = {
			'st_atime': self.atime,
			'st_ctime': self.ctime,
			'st_gid': self.gid,
			'st_mode': self.mode,
			'st_mtime': self.mtime,
			'st_nlink': self.nlink,
			'st_size': self.size,
			'st_uid': self.uid
		}
		return result

	@staticmethod
	def get_by_path(path):
		file_folder, file_name = FSNode._parse_folder_and_file_from_path(path.lstrip("/"))

		if file_folder not in FSNode._fsdata:
			FSNode._update_cache_for_object(file_folder)
		if file_folder in FSNode._fsdata:
			folder = FSNode._fsdata[file_folder]
			if file_name not in folder:
				FSNode._update_cache_for_object(path)
			if file_name in folder:
				return folder[file_name]
		return None

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
		if self.path in FSNode._fsdata:
			folder = FSNode._fsdata[self.path]
		else:
			return []
		dir_files = [folder[f] for f in folder.keys() if not f == ""]
		current_time = time.time()
		return [fsnode for fsnode in dir_files if not fsnode.deleted_on or fsnode.deleted_on > current_time]

	def save(self):
		"""
		This function should be overridden in any subclass that needs to perform an explicit save to end a transaction or
		flush changes to disk
		"""
		if self.folder in self._fsdata:
			self._fsdata[self.folder][self.name] = self
		else:
			self._fsdata[self.folder] = {self.name: self}
		if self.is_directory() and not self.path in FSNode._fsdata:
			FSNode._fsdata[self.path] = {}

	@staticmethod
	def set_swift_connection(swift_connection):
		FSNode._swift_connection = swift_connection

	def update_from_cache(self, path, cache_path):
		# split the file name out from its parent directory
		file_folder, file_name = FSNode._parse_folder_and_file_from_path(path)

		cached_st = os.lstat(cache_path)
		cached_attr = dict((key, getattr(cached_st, key)) for key in ('st_atime', 'st_ctime',
			'st_gid', 'st_mode', 'st_mtime', 'st_nlink', 'st_size', 'st_uid'))

		link_source = None
		if os.path.islink(cache_path):
			link_source = os.readlink(cache_path)

		self.path = path.lstrip("/")
		self.name = file_name
		self.folder = file_folder.lstrip("/")
		self.mode = int(cached_attr['st_mode'])
		self.uid = int(cached_attr['st_uid'])
		self.gid = int(cached_attr['st_gid'])
		self.mtime = float(cached_attr['st_mtime'])
		self.atime = float(cached_attr['st_atime'])
		self.ctime = float(cached_attr['st_ctime'])
		self.nlink = int(cached_attr['st_nlink'])
		self.size = int(cached_attr['st_size'])
		self.link_source = link_source

	def update_from_swift(self, swift_obj):
		obj_metadata = swift_obj.get_metadata()

		# split the file name out from its parent directory
		file_folder, file_name = FSNode._parse_folder_and_file_from_path(swift_obj.name)
		if file_folder not in FSNode._fsdata:
			FSNode._fsdata[file_folder] = {}
		folder = FSNode._fsdata[file_folder]

		self.path = swift_obj.name.lstrip("/")
		self.name = file_name
		self.folder = file_folder.lstrip("/")
		self.mode = int(obj_metadata['x-object-meta-fs-mode'])
		self.uid = int(obj_metadata['x-object-meta-fs-uid'])
		self.gid = int(obj_metadata['x-object-meta-fs-gid'])
		self.mtime = float(obj_metadata['x-object-meta-fs-mtime'])
		self.atime = float(obj_metadata['x-object-meta-fs-atime'])
		self.ctime = float(obj_metadata['x-object-meta-fs-ctime'])
		self.nlink = int(obj_metadata['x-object-meta-fs-nlink'])
		self.size = int(obj_metadata['x-object-meta-fs-size'])
		self.dirty = 0

		if 'x-object-meta-fs-deleted-on' in obj_metadata:
			self.deleted_on = obj_metadata['x-object-meta-fs-deleted-on']

		if 'x-object-meta-fs-link-source' in obj_metadata:
			self.link_source = obj_metadata['x-object-meta-fs-link-source']

	@staticmethod
	def _parse_folder_and_file_from_path(path):
		path_data = path.lstrip("/").rsplit('/', 1)
		if len(path_data) == 1:
			file_folder = ""
			file_name = path_data[0]
		else:
			file_folder = path_data[0]
			file_name = path_data[1]
		return (file_folder, file_name)

	@staticmethod
	def _update_cache_for_object(path):
		file_folder, file_name = FSNode._parse_folder_and_file_from_path(path)
		try:
			obj = FSNode._swift_connection.get_object(path)
		except Exception, e:
			return None
		if obj is not None:
			node = FSNode()
			node.update_from_swift(obj)
			node.save()
			FSNode._fsdata[file_folder][file_name] = node


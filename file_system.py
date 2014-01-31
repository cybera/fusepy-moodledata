from fsnode import File, Directory, FSNode

import os
import metadata

class FileSystem:
	def __init__(self, cache_root):
		self.access_wrappers = {}
		self.cache_root = cache_root

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



from file import File
import os

class FileSystem:
	def __init__(self, cache_root):
		self.open_files = {}
		self.cache_root = cache_root

	def get(self, path):
		if path not in self.open_files:
			self.open_files[path] = File(path, self)
		return self.open_files[path]

	def cache_path(self, path):
		return os.path.join(self.cache_root, path.lstrip("/"))
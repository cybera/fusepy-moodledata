from __future__ import with_statement

from errno import EACCES
from os.path import realpath

import os

from fuse import FuseOSError, Operations, LoggingMixIn
from file_system import FileSystem


class Moodledata(LoggingMixIn, Operations):
	def __init__(self, config):
		self.cache_dir = realpath(config["cache_dir"])
		self.config = config
		self.file_system = FileSystem(self.cache_dir)

	def access(self, path, mode):
		return self.get_file(path).access(mode)

	def chmod(self, path, mode):
		return self.get_file(path).chmod(mode)

	def chown(self, path, uid, gid):
		return self.get_file(path).chown(uid, gid)

	def create(self, path, mode):
		return self.get_file(path).create(mode)

	def flush(self, path, fh):
		return self.get_file(path).flush(fh)

	def fsync(self, path, datasync, fh):
		return self.get_file(path).fsync(datasync, fh)

	def getattr(self, path, fh=None):
		return self.get_file(path).getattr(fh)

	def getxattr(self, path, name, position=0):
		return self.get_file(path).getxattr(name, position)
	
	def link(self, path, source):
		return self.get_file(path).link(source)

	def listxattr(self, path):
		return self.get_file(path).listxattr()

	def mkdir(self, path, mode):
		return self.get_file(path).mkdir(mode)

	def mknod(self, path, mode, dev):
		return self.get_file(path).mknod(mode, dev)

	def open(self, path, flags):
		return self.get_file(path).open(flags)

	def read(self, path, size, offset, fh):
		return self.get_file(path).read(size, offset, fh)

	def readdir(self, path, fh):
		return self.get_file(path).readdir(fh)

	def readlink(self, path):
		return self.get_file(path).readlink()

	def release(self, path, fh):
		return self.get_file(path).release(fh)

	def rename(self, path, new):
		return self.get_file(path).rename(path, new)

	def rmdir(self, path):
		return self.get_file(path).rmdir(path)

	def statfs(self, path):
		print "here: %s" % path
		return self.get_file(path).statfs()

	def symlink(self, path, source):
		return self.get_file(path).symlink(source)

	def truncate(self, path, length, fh=None):
		return self.get_file(path).truncate(length, fh)

	def unlink(self, path):
		return self.get_file(path).unlink()

	def utimens(self, path, times=None):
		return self.get_file(path).utimens(times)

	def write(self, path, data, offset, fh):
		return self.get_file(path).write(data, offset, fh)

	def get_file(self, path):
		return self.file_system.get(path)
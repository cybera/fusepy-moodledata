from __future__ import with_statement

from errno import EACCES
from os.path import realpath

import os

from fuse import FuseOSError, Operations, LoggingMixIn
from file_system import FileSystem
from swift_source import SwiftSource

import metadata


class Moodledata(LoggingMixIn, Operations):
	def __init__(self, config):
		self.cache_dir = realpath(config["cache_dir"])
		# TODO: take this out once the metadata is actually exclusively representing the state on SWIFT
		metadata.cache_path = self.cache_dir
		self.config = config
		swift_connection = SwiftSource(
			auth_url=config["swift.auth_url"],
			username=config["swift.username"],
			password=config["swift.password"],
			tenant_id=config["swift.tenant_id"],
			region_name=config["swift.region_name"],
			source_bucket=config["source_bucket"])

		self.file_system = FileSystem(self.cache_dir, swift_connection)

	def __call__(self, op, path, *args):
		retval = super(Moodledata, self).__call__(op, path, *args)

		print "called %s (path: %s | args: %s)" % (op, path, args)

		return retval

	def access(self, path, mode):
		return self.file_system.access(path, mode)

	def chmod(self, path, mode):
		return self.object_for_path(path).chmod(mode)

	def chown(self, path, uid, gid):
		return self.object_for_path(path).chown(uid, gid)

	def create(self, path, mode):
		return self.file_system.create(path, mode)

	def flush(self, path, fh):
		return self.object_for_path(path).flush(fh)

	def fsync(self, path, datasync, fh):
		return self.object_for_path(path).fsync(datasync, fh)

	def getattr(self, path, fh=None):
		return self.object_for_path(path).getattr(fh)

	def getxattr(self, path, name, position=0):
		return self.object_for_path(path).getxattr(name, position)
	
	def link(self, target, source):
		return self.file_system.link(target, source)

	def listxattr(self, path):
		return self.object_for_path(path).listxattr()

	def mkdir(self, path, mode):
		return self.file_system.mkdir(path, mode)

	def mknod(self, path, mode, dev):
		return self.file_system.mknod(path, mode, dev)

	def open(self, path, flags):
		return self.object_for_path(path).open(flags)

	def read(self, path, size, offset, fh):
		return self.object_for_path(path).read(size, offset, fh)

	def readdir(self, path, fh):
		return self.file_system.readdir(path, fh)

	def readlink(self, path):
		return self.object_for_path(path).readlink()

	def release(self, path, fh):
		return self.object_for_path(path).release(fh)

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

	def object_for_path(self, path):
		return self.file_system.get(path.lstrip("/"))
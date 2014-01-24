#!/usr/bin/env python

from StringIO import StringIO

from time import time
import logging
import os
import stat

from collections import defaultdict
from errno import ENOENT
from stat import S_IFDIR, S_IFLNK, S_IFREG
from sys import argv, exit
from time import time

import fuse
from fuse import FUSE
from fuse import FuseOSError
from fuse import Operations
from fuse import LoggingMixIn
from fuse import fuse_get_context

import swiftclient.client as swift

import pyrax, os


# i.e. "/"
MOUNT_CONTAINER = 'swift-fuse'
CACHE_LOCATION = '/tmp/swift-fuse'


class SwiftFuse(LoggingMixIn, Operations):
	"""
	Implementation Notes:

	Files and Directories
	Given Swift doesn't distinguish between files and dirs, directories should
	be represented by a trailing slash.  This should be transparent to the
	user.  i.e.

	$ mkdir foo

	will PUT an object called "foo/" in Swift.
	"""

	def __init__(self):
		self.old_swift_client = swift.Connection(authurl=os.environ["OS_AUTH_URL"], user=os.environ["OS_USERNAME"],
											 key=os.environ["OS_PASSWORD"],
											 auth_version=2,
											 tenant_name=os.environ["OS_TENANT_NAME"], insecure=True)

		pyrax.settings.set('identity_type', 'keystone')
		pyrax.set_setting("auth_endpoint", os.environ["OS_AUTH_URL"])
		pyrax.set_credentials(username=os.environ["OS_USERNAME"], api_key=os.environ["OS_PASSWORD"], tenant_id=os.environ["OS_TENANT_ID"])
		self.swift_client = pyrax.connect_to_cloudfiles(os.environ["OS_REGION_NAME"])
		self.swift_mount = self.swift_client.get_container(MOUNT_CONTAINER)

	def getattr(self, path, fh=None):
		print "calling getattr"
		"""
		http://sourceforge.net/apps/mediawiki/fuse/index.php?
			title=Getattr%28%29
		"""
		st = {}
		if path.endswith('/'):
			st['st_mode'] = S_IFDIR | 0755
			st['st_nlink'] = 2  # . and .. at a minimum
		else:
			try:
				print "checking"
				obj = self.swift_mount.get_object(path.lstrip('/'))
				print "done checking"
				st['st_size'] = obj.total_bytes
			except pyrax.exc.NoSuchObject:
				st['st_size'] = 0

			st['st_mode'] = stat.S_IFREG | 0666
			st['st_nlink'] = 1

		st['st_ctime'] = time()
		st['st_mtime'] = st['st_ctime']
		st['st_atime'] = st['st_ctime']
		print "attr:"
		print st
		return st

	def readdir(self, path, fh):
		print "calling readdir"
		objects = self.swift_mount.get_object_names()
		contents = ['.', '..']
		for obj in objects:
			contents.append(obj.split('/')[0].rstrip('/'))
		return contents

	def open(self, path, flags):
		print "open: %s" % path
		full_path = "%s/%s" % (CACHE_LOCATION, path.lstrip('/'))
		if not os.path.isfile(full_path) and (flags & os.O_WRONLY) == 0:
			try:
				obj = self.swift_mount.get_object(path.lstrip('/'))
				with open(full_path, 'w') as cache_file:
					cache_file.write(obj.fetch())
			except pyrax.exc.NoSuchObject:
				pass
		print "about to open: %s" % full_path

		fh = os.open(full_path, flags)

		print "open fh: %s" % fh

		return fh

	def access(self, path, mode):
		print "calling access"
		if not os.access(path, mode):
			raise FuseOSError(EACCES)

	def create(self, path, mode):
		print "calling create"
		full_path = "%s/%s" % (CACHE_LOCATION, path.lstrip('/'))
		print "create: %s" % path
		return os.open(full_path, os.O_WRONLY | os.O_CREAT, mode)

	def read(self, path, size, offset, fh):
		print "calling read: %s" % fh
		# TODO:
		# if it exists in cache location:
		# 	read from cache location as regular file
		# else
		#	1. start incremental download on another thread (configurable increments based on
		#	bandwidth/performance)
		#	2. if enough of the file is downloaded to fulfill the requested read
		#		return the data
		#	else
		#		wait
		full_path = "%s/%s" % (CACHE_LOCATION, path.lstrip('/'))
		# if not os.path.isfile(full_path):
		# 	try:
		# 		obj = self.swift_mount.get_object(path.lstrip('/'))
		# 		with open(full_path, 'w') as cache_file:
		# 			cache_file.write(obj.fetch())
		# 	except pyrax.exc.NoSuchObject:
		# 		pass

		# if fh == 0:
		# 	fh = os.open(full_path, os.O_RDONLY)
		# 	print "fh: %s" % fh

		os.lseek(fh, offset, os.SEEK_SET)
		return os.read(fh, size)

	def release(self, path, fh):
		print "calling release: %s" % fh

		return os.close(fh)

	def mkdir(self, path, mode):
		print "calling mkdir"

		# use trailing slash to indicate a dir
		path = path.lstrip('/').rstrip('/') + '/'
		headers, body = self.old_swift_client.put_object(
				MOUNT_CONTAINER, path, None)
		return 0

	def statfs(self, path):
		print "calling statfs"

		return dict(f_bsize=512, f_blocks=4096, f_bavail=2097152)

	def chmod(self, path, mode):
		print "calling chmod"

		return 0                # FIXME not really!
		return -errno.ENOSYS

	def chown(self, path, uid, gid):
		print "calling chown"

		return 0                # FIXME not really!
		return -errno.ENOSYS

	def write(self, path, data, offset, fh):
		print "calling write"

		os.lseek(fh, offset, os.SEEK_SET)
		bytes_written = os.write(fh, data)

		# self.swift_mount.store_object(path.lstrip('/'), data)
		return bytes_written
	def flush(self, path, fh):
		print "calling flush: %s" % fh

		os.fsync(fh)
		# cached_path = "%s/%s" % (CACHE_LOCATION, path.lstrip('/'))
		# self.swift_mount.upload_file(path.lstrip('/'), cached_path)

	def fsync(self, path, datasync, fh):
		print "calling fsync: %s (%s)" % (path, fh)
		# cached_path = "%s/%s" % (CACHE_LOCATION, path.lstrip('/'))
		# self.swift_mount.upload_file(path.lstrip('/'), cached_path)
		return os.fsync(fh)

	def rename(self, old, new):
		print "calling rename"

		return 0
	def rmdir(self, path):
		print "calling rmdir"

		return 0
	def truncate(self, path, length, fh=None):
		print "calling truncate"

		return 0
	def utimens(self, path, times=None):
		print "calling utimens"

		return 0



if __name__ == '__main__':
	if len(argv) != 2:
		print('usage: %s <mountpoint>' % argv[0])
		exit(1)

	logging.getLogger().setLevel(logging.DEBUG)
	fuse = FUSE(SwiftFuse(), argv[1], foreground=True)
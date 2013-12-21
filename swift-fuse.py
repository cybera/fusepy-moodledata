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
				obj = self.swift_mount.get_object(path.lstrip('/'))
				st['st_size'] = obj.total_bytes
			except pyrax.exceptions.NoSuchObject:
				st['st_size'] = 0

			st['st_mode'] = stat.S_IFREG | 0666
			st['st_nlink'] = 1
			
		st['st_ctime'] = time()
		st['st_mtime'] = st['st_ctime']
		st['st_atime'] = st['st_ctime']
		return st

	def readdir(self, path, fh):
		headers, objects = self.old_swift_client.get_container(MOUNT_CONTAINER)
		contents = ['.', '..']
		for obj in objects:
			contents.append(obj['name'].split('/')[0].rstrip('/'))
		return contents

	def create(self, path, mode):
		try:
			print "in create: %s" % path
			self.old_swift_client.put_object(MOUNT_CONTAINER, path.lstrip('/'),
										 None)
		except swift.ClientException as e:
			raise
		return 0

	def read(self, path, size, offset, fh):
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
		obj = self.swift_mount.get_object(path.lstrip('/'))
		return obj.fetch()

	def mkdir(self, path, mode):
		# use trailing slash to indicate a dir
		path = path.lstrip('/').rstrip('/') + '/'
		headers, body = self.old_swift_client.put_object(
				MOUNT_CONTAINER, path, None)
		return 0

	def statfs(self, path):
		return dict(f_bsize=512, f_blocks=4096, f_bavail=2097152)

	def chmod(self, path, mode):
		return 0                # FIXME not really!
		return -errno.ENOSYS

	def chown(self, path, uid, gid):
		return 0                # FIXME not really!
		return -errno.ENOSYS

	def write(self, path, data, offset, fh):
		# TODO:
		# write to cache location
		# check for EOF in data
		# when EOF, sync to SWIFT
		# also sync to SWIFT when fsync() called
		self.swift_mount.store_object(path.lstrip('/'), data)
		return len(data)

	def rename(self, old, new):
		return 0
	def rmdir(self, path):
		return 0
	def truncate(self, path, length, fh=None):
		return 0
	def utimens(self, path, times=None):
		return 0



if __name__ == '__main__':
	if len(argv) != 2:
		print('usage: %s <mountpoint>' % argv[0])
		exit(1)

	logging.getLogger().setLevel(logging.DEBUG)
	fuse = FUSE(SwiftFuse(), argv[1], foreground=True)
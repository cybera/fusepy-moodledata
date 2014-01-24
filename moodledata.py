#!/usr/bin/env python

import logging

from collections import defaultdict
from errno import ENOENT
from stat import S_IFDIR, S_IFLNK, S_IFREG
from sys import argv, exit
from time import time

from fuse import FUSE, FuseOSError, Operations, LoggingMixIn

import httplib

import pyrax, os

if not hasattr(__builtins__, 'bytes'):
	bytes = str

class Moodledata(LoggingMixIn, Operations):
	'Example memory filesystem. Supports only one level of files.'

	def __init__(self, swift_url):
		self.files = {}
		self.swift_url = swift_url
		self.data = defaultdict(bytes)
		self.fd = 0
		now = time()
		self.files['/'] = dict(st_mode=(S_IFDIR | 0755), st_ctime=now,
							   st_mtime=now, st_atime=now, st_nlink=2)

	def create(self, path, mode):
		self.files[path] = dict(st_mode=(S_IFREG | mode), st_nlink=1,
								st_size=0, st_ctime=time(), st_mtime=time(),
								st_atime=time())

		self.fd += 1
		return self.fd

	def getattr(self, path, fh=None):
		# from https://github.com/brk3/swift-fuse/blob/master/swift-fuse.py
		"""
		http://sourceforge.net/apps/mediawiki/fuse/index.php?title=Getattr%28%29
		"""
		st = {}
		if path.endswith('/'):
			st['st_mode'] = S_IFDIR | 0755
			st['st_nlink'] = 2  # . and .. at a minimum
		else:
			st['st_mode'] = S_IFREG | 0666
			st['st_nlink'] = 1
			# TODO: extract size from swift
			st['st_size'] = 1
		st['st_ctime'] = time()
		st['st_mtime'] = st['st_ctime']
		st['st_atime'] = st['st_ctime']
		return st

	def mkdir(self, path, mode):
		self.files[path] = dict(st_mode=(S_IFDIR | mode), st_nlink=2,
								st_size=0, st_ctime=time(), st_mtime=time(),
								st_atime=time())

		self.files['/']['st_nlink'] += 1

	def read(self, path, size, offset, fh):
		conn = httplib.HTTPConnection(self.swift_url)
		conn.request("GET", path)
		response = conn.getresponse().read()
		with open(response) as f:
			f.seek(offset, 0)
			return f.read(size)
		return ''

	def readdir(self, path, fh):
		conn = httplib.HTTPConnection(self.swift_url)
		conn.request("GET", path)
		response = conn.getresponse().read()
		items = response.split("\n")

		return ['.', '..'] + [x.rstrip('/') for x in items]

	def rmdir(self, path):
		self.files.pop(path)
		self.files['/']['st_nlink'] -= 1




if __name__ == '__main__':
	if len(argv) != 3:
		print('usage: %s <swift url> <mountpoint>' % argv[0])
		exit(1)

	logging.getLogger().setLevel(logging.DEBUG)
	fuse = FUSE(Moodledata(argv[1]), argv[2], foreground=True)
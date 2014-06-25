#!/usr/bin/env python

# from moodledata import Moodledata
from sys import argv, exit
from file_system import FileSystem
import logging, logging.config
import os

from swift_source import SwiftSource
from config import Config

def upload_path(base, path):
	full_path = os.path.join(base, path)
	if os.path.isdir(full_path):
		for item in os.listdir(full_path):
			item_path = os.path.join(path, item)
			#print "processing %s"% item_path
			upload_file(base, item_path)
			if os.path.isdir(os.path.join(full_path, item)):
				upload_path(base, item_path)
	else: # function called on file instead of directory, so just upload
		upload_file(base, path)
	return

def upload_file(base, path):
	def callback(success, error_message):
		pass
	node = FSNode(base, path)
	swift_connection.update_object(node, base, callback)

class FSNode:
	'''
	This is a dumbed down version of the FSNode class used for Fuse
	'''
	def __init__(self, base, path):
		full_path = os.path.join(base, path)

		# split the file name out from its parent directory
		file_folder, file_name = FSNode._parse_folder_and_file_from_path(path)

		st = os.lstat(full_path)
		attr = dict((key, getattr(st, key)) for key in ('st_atime', 'st_ctime',
			'st_gid', 'st_mode', 'st_mtime', 'st_nlink', 'st_size', 'st_uid'))

		link_source = None
		if os.path.islink(full_path):
			link_source = os.readlink(full_path)

		self.path = path.lstrip("/")
		self.name = file_name
		self.folder = file_folder.lstrip("/")
		self.mode = int(attr['st_mode'])
		self.uid = int(attr['st_uid'])
		self.gid = int(attr['st_gid'])
		self.mtime = float(attr['st_mtime'])
		self.atime = float(attr['st_atime'])
		self.ctime = float(attr['st_ctime'])
		self.nlink = int(attr['st_nlink'])
		self.size = int(attr['st_size'])
		self.link_source = link_source

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


if __name__ == '__main__':
	if len(argv) == 2:
		md_config = Config(argv[1])
	else:
		md_config = Config()
	logging.config.fileConfig('logging.conf')

	config = md_config

	swift_connection = SwiftSource(
		auth_url=config["swift.auth_url"],
		username=config["swift.username"],
		password=config["swift.password"],
		tenant_id=config["swift.tenant_id"],
		region_name=config["swift.region_name"],
		source_bucket=config["source_bucket"])

	# get directory
	#upload_path("/usr/local/lib/python2.7", "dist-packages")
	upload_path("/home/ubuntu/random/test", "")
	print "------------waiting for files to upload to swift----------------"
	swift_connection.terminate_workers()
	exit()


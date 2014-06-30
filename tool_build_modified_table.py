#!/usr/bin/env python

# from moodledata import Moodledata
import argparse
from sys import argv, exit
import json
import logging, logging.config
import os
import pyrax.utils as utils

from config import Config

def process_path(base, path):
	full_path = os.path.join(base, path)
	if os.path.isdir(full_path):
		for item in os.listdir(full_path):
			item_path = os.path.join(path, item)
			add_entry(base, item_path)
			if os.path.isdir(os.path.join(full_path, item)):
				process_path(base, item_path)
	else: # function called on file instead of directory, so just upload
		add_entry(base, path)
	return

def add_entry(base, path):
	node = FSNode(base, path)
	if path in manifest and node.mtime <= manifest[path]["mtime"]:
		return
	print "noype"
	file_hash = utils.get_checksum(os.path.join(base, path))
	manifest[path] = ({
			"md5": file_hash,
			"atime": node.atime,
			"ctime": node.ctime,
			"mtime": node.mtime
			})

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
	parser = argparse.ArgumentParser()
	parser.add_argument("-m", "--file_manifest", help="The file containing md5-sum and other data")
	parser.add_argument("-c", "--config", help="The config file to be used")
	parser.add_argument("-u", "--upload_path", help="The path to sync the Swift container to")
	args = parser.parse_args()

	
	config = Config(args.config) if args.config else Config()
	logging.config.fileConfig('logging.conf')

	file_manifest_path = args.file_manifest

	manifest = {}

	if os.path.isfile(file_manifest_path):
		with open(file_manifest_path, 'r') as json_file:
			manifest = json.load(json_file)
	# get directory
	process_path("/usr/local/lib/python2.7", "dist-packages")
	with open(file_manifest_path, 'w') as json_file:
		json_file.write(json.dumps(manifest))
	exit()


#!/usr/bin/env python

# from moodledata import Moodledata
from sys import argv, exit
from fuse import FUSE
from file_system import FileSystem
import logging, logging.config

from config import Config

if __name__ == '__main__':
	if len(argv) == 2:
		md_config = Config(argv[1])
	else:
		md_config = Config()
	logging.config.fileConfig('logging.conf')
	fuse = FUSE(FileSystem(md_config), md_config["mount_dir"], foreground=True, allow_other=True)


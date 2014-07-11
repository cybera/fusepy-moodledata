import os

import logging

def init_cache(cache_root):
	'''
	This simply creates cache directories to simplify other file operations.
	NOTE: this only makes sense when you know before-hand all (or a significant
	      subset) of the directories that should exist in the mount.
	'''
	for i in range(256):
		directory = os.path.join(cache_root, "%0.2x" % i)
		if not os.path.exists(directory):
			os.mkdir(directory)
		for j in range(256):
			directory = os.path.join(directory, "%0.2x" % j)
			if not os.path.exists(directory):
				os.mkdir(directory)
	

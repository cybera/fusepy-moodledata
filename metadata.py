import os

def exists(path):
	# TODO: This should actually be a query to the sqlite database
	return os.path.exists(globals()['cache_path'] + path)

def is_directory(path):
	# TODO: This should actually be a query to the sqlite database
	print "is directory? (%s): %s" % (path, os.path.isdir(globals()['cache_path'] + path))
	return os.path.isdir(globals()['cache_path'] + path)
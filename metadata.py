import os
import sqlite3
import threading

def exists(path):
	# TODO: This should actually be a query to the sqlite database
	return os.path.exists(globals()['cache_path'] + path)

def is_directory(path):
	# TODO: This should actually be a query to the sqlite database
	print "is directory? (%s): %s" % (path, os.path.isdir(globals()['cache_path'] + path))
	return os.path.isdir(globals()['cache_path'] + path)

class Database:
	def __init__(self, database_path):
		# TODO actual sqlite3 connection
		print "init thread: %s" % threading.current_thread()
		self.database_path = database_path
		self.connections = {}

	def select(self, query, *parameters):
		print "select thread: %s" % threading.current_thread()
		cursor = self.__connection__().cursor()
		return [row for row in cursor.execute(query, parameters)]

	def update(self, query, *parameters):
		cursor = self.__connection__().cursor()
		cursor.execute(query, parameters)
		self.__connection__().commit()

	def __connection__(self):
		current_thread_id = threading.current_thread().name

		if current_thread_id not in self.connections:
			self.connections[current_thread_id] = sqlite3.connect(self.database_path)

		# clean up any connections for non-existent threads
		active_thread_names = [active_thread.name for active_thread in threading.enumerate()]
		for thread_name in self.connections.keys():
			if thread_name not in active_thread_names:
				del self.connections[thread_id]

		return self.connections[current_thread_id]
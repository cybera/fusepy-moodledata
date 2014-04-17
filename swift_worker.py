from functools import wraps
import pyrax
import logging
import os
import multiprocessing
from random import randint

from swiftclient import client as _swift_client

class SwiftWorker(multiprocessing.Process):
	def __init__(self, task_queue, response_queue, auth_url, username, password, tenant_id, region_name, source_bucket, max_attempts = 5):
		multiprocessing.Process.__init__(self)
		self.logger = logging.getLogger('swift_worker')
		self.task_queue = task_queue
		self.response_queue = response_queue
		pyrax.settings.set('identity_type', 'keystone')
		pyrax.set_setting("auth_endpoint", auth_url)
		pyrax.set_credentials(username=username, api_key=password, tenant_id=tenant_id)
		self.swift_client = pyrax.connect_to_cloudfiles(region_name)
		self.swift_mount = self.swift_client.get_container(source_bucket)
		self.max_attempts = max_attempts

	def handle_client_exception(fnc):
		"""
		Here we wrap all our functions that actually make requests to Swift to do some generic
		exception handling. If an exception can not be handled generically then we just re-raise it.
		"""
		@wraps(fnc)
		def _wrapped(self, *args, **kwargs):
			attempts = 0

			while attempts < self.max_attempts:
				attempts += 1
				try:
					ret = fnc(self, *args, **kwargs)
					return ret
				except _swift_client.ClientException, e:
					if e.http_status == 500:
						self.logger.error('''"worker":"%s", "message":"500 error, attempt %d: %s"''', self.name, attempts, e.message)
						continue
					else:
						raise
				except:
					raise
		return _wrapped

	def run(self):
		"""
		This is the main event loop for our worker threads.
		When there is a task we check that all arguements are provided and then pass things off
		to the appropriate handler. If an exception is raised, or the task otherwise fails we
		simply pass the failure information into the response queue and let whichever callback
		was provided handle the failure.
		"""
		stay_alive = True
		while stay_alive:
			self.logger.debug('''"worker":"%s", "message":"waiting for task"''', self.name)
			task = self.task_queue.get()
			task_success = True
			task_error_message = None
			if task.command == "download_object":
				if "object_name" in task.args.keys() and "destination_path" in task.args.keys():
					object_name = task.args["object_name"]
					destination_path = task.args["destination_path"]
					self.logger.debug('''"worker":"%s", "message":"downloading object '%s'"''', self.name, object_name)
					try:
						task_success = self.download_object(object_name, destination_path)
						if not task_success:
							task_error_message = "unable to download object"
					except Exception, e:
						task_success = False
						task_error_message = e.message
				else:
					task_success = False
					task_error_message = "missing arguments in 'download_object' command"
			elif task.command == "create_object":
				if "object_name" in task.args.keys() and "source_path" in task.args.keys():
					object_name = task.args["object_name"]
					source_path = task.args["source_path"]
					metadata = task.args["metadata"] if ("metadata" in task.args.keys()) else {}
					self.logger.debug('''"worker":"%s", "message":"creating object '%s'"''', self.name, object_name)
					try:
						task_success = self.create_object(object_name, source_path, metadata)
						if not task_success:
							task_error_message = "unable to create object"
					except Exception, e:
						task_success = False
						task_error_message = e.message
				else:
					task_success = False
					task_error_message = "missing arguments in 'upload_object' command"
			elif task.command == "move_object":
				if "source" in task.args.keys() and "destination" in task.args.keys():
					source = task.args["source"]
					destination = task.args["destination"]
					self.logger.debug('''"worker":"%s", "message":"moving object '%s' to '%s'"''', self.name, source, destination)
					try:
						task_success = self.move_object(source, destination)
						if not task_success:
							task_error_message = "unable to move object"
					except Exception, e:
						task_success = False
						task_error_message = e.message
				else:
					task_success = False
					task_error_message = "missing arguments in 'move_object' command"

			elif task.command == "set_object_metadata":
				if "object_name" in task.args.keys() and "metadata" in task.args.keys():
					object_name = task.args["object_name"]
					metadata = task.args["metadata"]
					self.logger.debug('''"worker":"%s", "message":"setting metadata for object '%s'"''', self.name, object_name)
					try:
						task_success = self.set_object_metadata(object_name, metadata)
						if not task_success:
							task_error_message = "unable to set metadata for object"
					except Exception, e:
						task_success = False
						task_error_message = e.message
				else:
					task_success = False
					task_error_message = "missing arguments in 'set_metadata' command"
				
			elif task.command == "shutdown":
				# TODO: this would probably be best handled by dealing with a process signal
				self.logger.debug('''"worker":"%s", "message":"worker thread shutting down"''', self.name)
				stay_alive = False
			else:
				self.logger.debug('''"worker":"%s", "message":"invalid job command"''', self.name)
				task_success = False
				task_error_message = "Invalid command"
			self.task_queue.task_done()
			response = SwiftResponse(task.job_id, task_success, task_error_message)
			self.response_queue.put(response)

			if task_success:
				self.logger.debug('''"worker":"%s", "message":"task successful"''', self.name)
			else:
				self.logger.debug('''"worker":"%s", "message":"task failed: %s"''', self.name, task_error_message)

	@handle_client_exception
	def download_object(self, object_name, destination_path):
		# TODO: chunk size should be an attribute in the config file... magic number... bad
		chunk_size = 1024*1024 # 1MB chunks
		try:
			fp = open(destination_path, 'wb')
		except IOError, e:
			return e
		obj = self.swift_mount.get_object(object_name, cached=False)
		if obj == None:
			return False
		for chunk in obj.get(chunk_size=chunk_size):
			fp.write(chunk)
		# TODO: at this point it is probably a good idea to check the file size and make
		#       sure that the MD5 hash matches. If either of these do not check out, we
		#       should return false
		fp.close()
		return True

	@handle_client_exception
	def create_object(self, object_name, source_path, metadata):
		"""
		Creates the specified object in Swift. If the source_path points to a file
		then we upload the file, otherwise, we upload an empty object.
		Will return true iff the returned http status code is 201 (Created)

		NOTE: pyrax's upload_file function takes care of segmenting files if they
				  exceed the max object size.
		"""
		upload_response = {}
		if os.path.isfile(source_path):
			# TODO: we currently can't use the swift object's upload_file as it does not
			#				accept the extra_info arguement (this could be fixed by submitting
			#				a pull request to pyrax
			obj = self.swift_client.upload_file(self.swift_mount, source_path, 
					obj_name = object_name, extra_info = upload_response)
		else:
			data = ""
			obj = self.swift_mount.store_object(object_name, data, extra_info = upload_response)
		obj.set_metadata(metadata)
		# TODO: if we want to be really paraniod we can verify the MD5 hash of the uploaded object
		#				this is the 'etag' value in upload_response
		# TODO: right now our error checking consists of making sure we get a 201 http response
		#				this could be enough, but really this deserves more research.
		return upload_response['status'] == 201

	@handle_client_exception
	def move_object(self, source, destination):
		"""
		Moves the file from source to destination in the same container.
		This really ends up renaming the object.
		Will return true iff the returned http status code is 201 (Created)
		"""
		upload_response = {}
		obj = self.swift_mount.get_object(source)
		if obj:
			try:
				obj.move(obj.container, destination.lstrip("/"), extra_info = upload_response)
			except Exception, e:
				# Oddly every once and a while a 'object/container does not exist' exception is raised even though
				# the swift call responds with the expected HTTP 201 code. So we re-raise the exception if the move
				# command did in fact fail
				# TODO: this definitely warrents more investigation. In the testing done so far it really looks like
				#       the above comment is correct, but we should be sure...
				if upload_response['status'] != 201:
					raise
			# TODO: right now our error checking consists of making sure we get a 201 http response
			#				this could be enough, but really this deserves more research.
			return upload_response['status'] == 201
		else:
			return False
	
	@handle_client_exception
	def set_object_metadata(self, object_name, metadata):
		"""
		Sets the metadata for the given object.
		Will return true iff the returned http status code is 202 (Accepted)
		"""
		call_response = {}
		obj = self.swift_mount.get_object(object_name)
		if obj:
			self.swift_client.set_object_metadata(self.swift_mount, obj, metadata, extra_info = call_response)
			# TODO: right now our error checking consists of making sure we get a 201 http response
			#				this could be enough, but really this deserves more research.
			return call_response['status'] == 202
		else:
			return False

class SwiftTask(object):
	'''
	SwiftTask is used to kick off a SwiftWorker job.
	Arguments:
		- job_id: The ID of the job
		- command: the command name you want the swift worker to execute
		- args: a dict that contains any needed arguments for the command
	'''
	def __init__(self, command, args):
		self.job_id = randint(0,1000000000)
		self.command = command
		self.args = args

class SwiftResponse(object):
	'''
	SwiftResponse is used to communicate the result of a SwiftWorker job.
	Arguments:
		- job_id: The ID of the job
		- success: boolean value indicating if the job succeeded or not
		- error_messagee: if the job did not succeed an optional message can be attached here
	'''
	def __init__(self, job_id, success, error_message=""):
		self.job_id = job_id
		self.success = success
		self.error_message = error_message


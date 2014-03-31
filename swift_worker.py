import pyrax
import os
import multiprocessing
from random import randint

class SwiftWorker(multiprocessing.Process):
	def __init__(self, task_queue, response_queue, auth_url, username, password, tenant_id, region_name, source_bucket):
		multiprocessing.Process.__init__(self)
		self.task_queue = task_queue
		self.response_queue = response_queue
		pyrax.settings.set('identity_type', 'keystone')
		pyrax.set_setting("auth_endpoint", auth_url)
		pyrax.set_credentials(username=username, api_key=password, tenant_id=tenant_id)
		self.swift_client = pyrax.connect_to_cloudfiles(region_name)
		self.swift_mount = self.swift_client.get_container(source_bucket)

	# worker process enters a command loop until it receives the 'shutdown' command
	def run(self):
		stay_alive = True
		while stay_alive:
			print "WORKER %s: waiting for task" % self.name
			task = self.task_queue.get()
			print "WORKER %s: have task, ok, lets go" % self.name
			task_success = True
			task_error_message = None
			if task.command == "download_object":
				print "WORKER %s: time to download an object" % self.name
				if "object_name" in task.args.keys() and "destination_path" in task.args.keys():
					object_name = task.args["object_name"]
					destination_path = task.args["destination_path"]
					task_success = self.download_object(object_name, destination_path)
				else:
					task_success = False
					task_error_message = "missing arguments in 'download_object' command"
				if task_success:
					print "WORKER %s: Successfully downloaded object" % self.name
				else:
					print "WORKER %s: unable to download object" % self.name
			elif task.command == "create_object":
				print "WORKER %s: creating object" % self.name
				if "object_name" in task.args.keys() and "source_path" in task.args.keys():
					object_name = task.args["object_name"]
					source_path = task.args["source_path"]
					metadata = task.args["metadata"] if ("metadata" in task.args.keys()) else {}
					task_success = self.create_object(object_name, source_path, metadata)
					if not task_success:
						task_error_message = "error uploading object"
				else:
					task_success = False
					task_error_message = "missing arguments in 'upload_object' command"
				if task_success:
					print "WORKER %s: object created successfully" % self.name
				else:
					print "WORKER %s: unable to create object" % self.name

			elif task.command == "move_object":
				print "WORKER %s: moving object" % self.name
				if "source" in task.args.keys() and "destination" in task.args.keys():
					source = task.args["source"]
					destination = task.args["destination"]
					task_success = self.move_object(source, destination)
					if not task_success:
						task_error_message = "error moving object"
				else:
					task_success = False
					task_error_message = "missing arguments in 'move_object' command"
				if task_success:
					print "WORKER %s: object moved successfully" % self.name
				else:
					print "WORKER %s: failed to move object" % self.name

			elif task.command == "set_object_metadata":
				print "WORKER %s: setting metadata for object" % self.name
				if "object_name" in task.args.keys() and "metadata" in task.args.keys():
					object_name = task.args["object_name"]
					metadata = task.args["metadata"]
					task_success = self.set_object_metadata(object_name, metadata)
					if not task_success:
						task_error_message = "error setting metadata for object"
				else:
					task_success = False
					task_error_message = "missing arguments in 'set_metadata' command"
				if task_success:
					print "WORKER %s: successfully set metadata for object" % self.name
				else:
					print "WORKER %s: unable to set metadata for object" % self.name
				
			elif task.command == "shutdown":
				print "WORKER %s: swift, power.... dowwwwnnnnnnnn" % self.name
				stay_alive = False
			else:
				print "WORKER %s: does not compute" % self.name
				task_success = False
				task_error_message = "Invalid command"
			self.task_queue.task_done()
			response = SwiftResponse(task.job_id, task_success, task_error_message)
			self.response_queue.put(response)

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

	def move_object(self, source, destination):
		"""
		Moves the file from source to destination in the same container.
		This really ends up renaming the object.
		Will return true iff the returned http status code is 201 (Created)
		"""
		upload_response = {}
		obj = self.swift_mount.get_object(source)
		if obj:
			obj.move(obj.container, destination.lstrip("/"), extra_info = upload_response)
			# TODO: right now our error checking consists of making sure we get a 201 http response
			#				this could be enough, but really this deserves more research.
			return upload_response['status'] == 201
		else:
			return False
	
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


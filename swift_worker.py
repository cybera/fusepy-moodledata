import pyrax
import os
import multiprocessing
from random import randint

# TODO: need to send SwiftResponse object back to the response queue to indicate success or failure
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
			elif task.command == "create_object":
				print "WORKER %s: creating object" % self.name
				if "object_name" in task.args.keys() and "source_path" in task.args.keys():
					object_name = task.args["object_name"]
					source_path = task.args["source_path"]
					metadata = task.args["metadata"] if ("metadata" in task.args.keys()) else {}
					task_success = self.create_object(object_name, source_path, metadata)
				else:
					task_success = False
					task_error_message = "missing arguments in 'upload_object' command"
				print "WORKER %s: object created successfully" % self.name
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
		try:
			fp = open(destination_path, 'wb')
		except IOError, e:
			return e
		obj = self.swift_mount.get_object(object_name, cached=False)
		if obj == None:
			return "The object '%s' does not exist"
		for chunk in obj.get(chunk_size=self.chunk_size):
			fp.write(chunk)
		fp.close()
		return True

	def create_object(self, object_name, source_path, metadata):
		"""
		Creates the specified object in Swift. If the source_path points to a file
		then we upload the file, otherwise, we upload an empty object.

		NOTE: pyrax's upload_file function takes care of segmenting files if they
				  exceed the max object size.
		"""
		# TODO: Right now we always return true.... surely its possoble to fail here.  
		if os.path.isfile(source_path):
			obj = self.swift_mount.upload_file(source_path, obj_name = object_name)
		else:
			data = ""
			obj = self.swift_mount.store_object(object_name, data)
		obj.set_metadata(metadata)
		return True

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


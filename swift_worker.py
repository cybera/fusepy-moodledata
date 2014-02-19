import pyrax
import os
import multiprocessing, thread

class SwiftWorker(multiprocessing.Process):
	def __init__(self, task_queue, auth_url, username, password, tenant_id, region_name, source_bucket):
		multiprocessing.Process.__init__(self)
		self.task_queue = task_queue
		pyrax.settings.set('identity_type', 'keystone')
		pyrax.set_setting("auth_endpoint", auth_url)
		pyrax.set_credentials(username=username, api_key=password, tenant_id=tenant_id)
		self.swift_client = pyrax.connect_to_cloudfiles(region_name)
		self.swift_mount = self.swift_client.get_container(source_bucket)

	# worker process enters a command loop until it receives the 'shutdown' command
	def run(self):
		while True:
			print "WORKER %s: waiting for task" % self.name
			task = self.task_queue.get()
			print "WORKER %s: have task, ok, lets go" % self.name
			task_error = None
			if task.command == "download_object":
				print "WORKER %s: time to download an object" % self.name
				if "object_name" in task.args.keys() and "destination_path" in task.args.keys():
					object_name = task.args["object_name"]
					destination_path = task.args["destination_path"]
					task_error = self.download_object(object_name, destination_path)
				else:
					task_error = "missing arguments in 'download_object' command"
				self.task_queue.task_done()
			elif task.command == "create_object":
				print "WORKER %s: creating object" % self.name
				if "object_name" in task.args.keys() and "source_path" in task.args.keys():
					object_name = task.args["object_name"]
					source_path = task.args["source_path"]
					metadata = task.args["metadata"] if ("metadata" in task.args.keys()) else {}
					task_error = self.create_object(object_name, source_path, metadata)
				else:
					task_error = "missing arguments in 'upload_object' command"
				self.task_queue.task_done()
				print "WORKER %s: object created successfully" % self.name
			elif task.command == "shutdown":
				print "WORKER %s: swift, power.... dowwwwnnnnnnnn" % self.name
				self.task_queue.task_done()
				break
			else:
				print "WORKER %s: does not compute" % self.name
				self.task_queue.task_done()

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
		return None

	def create_object(self, object_name, source_path, metadata):
		# TODO: This needs to be way more efficient. Large files won't be able to be read into a single string.
		# TODO: We'll probably want to use the "upload file" functionality of pyrax instead for actual files
		data = ""
		if os.path.isfile(source_path):
			with open (source_path, "rb") as source_file:
				data = source_file.read()
		obj = self.swift_mount.store_object(object_name, data)
		obj.set_metadata(metadata)
		return None

class SwiftTask(object):
	'''
	SwiftTask is used to kick off a SwiftWorker job.
	Arguments:
		- command: the command name you want the swift worker to execute
		- args: a dict that contains any needed arguments for the command
	'''
	def __init__(self, command, args):
		self.command = command
		self.args = args


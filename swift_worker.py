import pyrax
import os
import multiprocessing, thread

'''
ok, need to keep track of what is being uploaded along with when we started the upload
^^^ probably also with downloads

IPC is a little tricky when we need to update the state of the SQLite database.... SwiftWorker shouldn't really have any
clue about the SQLite database.... which suggests a callback, but a callback is not serializable, which means we need a 'results queue'
(http://pymotw.com/2/multiprocessing/communication.html is going to be helpful there)

we'll also need a way of timing out a request.... but this is kinda tricky because different files will take different amounts of time to transmit.
much pondering to do on this... much indeed...
'''
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
		print "WORKER %s: entering event loop" % self.name
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
			elif task.command == "shutdown":
				print "WORKER %s: swift, power.... dowwwwnnnnnnnn" % self.name
				self.task_queue.task_done()
				break
			else:
				print "WORKER %s: does not compute" % self.name
				task.callback(error = "invalid swift worker command")
				self.task_queue.task_done()

		# ok, we've received the kill command, wrap things up cleanly here

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
		print "WORKER %s: in create_object function" % self.name
		data = ""
		if os.path.isfile(source_path):
			with open (source_path, "rb") as source_file:
				data = source_file.read()
		print "WORKER %s: create_object, have data...., %s" % (self.name, data)
		obj = self.swift_mount.store_object(object_name, data)
		print "WORKER %s: create_object, supposedly uploaded the object, hooray" % self.name
		obj.set_metadata(metadata)
		print "WORKER %s: create_object, set the metadata" % self.name
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


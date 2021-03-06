import logging
import time
import pyrax
import os
import multiprocessing
import thread
from swift_worker import SwiftWorker, SwiftTask, SwiftResponse

class SwiftSource:
	def __init__(self, auth_url, username, password, tenant_id, region_name, source_bucket):
		# TODO: now that we have swift workers, should we move away from having swift connections here?
		#       The advantage would be that we no longer would block on simple requests (which may or may not be a
		#       performance bottleneck)
		self.logger = logging.getLogger('swift')
		pyrax.settings.set('identity_type', 'keystone')
		pyrax.set_setting("auth_endpoint", auth_url)
		pyrax.set_credentials(username=username, api_key=password, tenant_id=tenant_id)
		self.swift_client = pyrax.connect_to_cloudfiles(region_name)
		self.swift_mount = self.swift_client.get_container(source_bucket)

		self.task_queue = multiprocessing.JoinableQueue()
		self.response_queue = multiprocessing.JoinableQueue()
		# TODO: the number of workers should be a setting in the config file
		self.num_workers = 20
		self.workers = [SwiftWorker(self.task_queue, self.response_queue, auth_url, username, password, tenant_id, region_name, source_bucket) for i in xrange(self.num_workers)]
		for worker in self.workers:
			worker.start()

		# TODO: do we need to keep this reference?
		self.active_job_callbacks = {}
		self.swift_response_thread = thread.start_new_thread(self._response_thread_main, ())

	def download_object(self, object_name, destination, callback):
		"""
		Downloads the specified object to the destionation
		If the file does not yet exist we create it. This will cover the case that the calling
		code needs to open the file for reading before the file is created.
		"""
		task = SwiftTask(command = "download_object",
				args = {
					"object_name": object_name,
					"destination_path": destination
					})
		self.active_job_callbacks[task.job_id] = callback
		self.task_queue.put(task)

	def get_object(self, path, cached=False):
		return self.swift_mount.get_object(path.lstrip("/"), cached)

	def get_objects(self, path):
		return self.swift_mount.get_objects(prefix = path.lstrip("/"))

	def set_object_metadata(self, path, metadata, callback):
		"""
		Sets the metadata for the object. The metadata argument should be a dict.
		"""
		task = SwiftTask(command = "set_object_metadata",
				args = {
					"object_name": path.lstrip("/"),
					"metadata": metadata
				})
		self.active_job_callbacks[task.job_id] = callback
		self.task_queue.put(task)

	def update_object(self, fsnode, cache_root, callback, md5sum = None):
		# TODO: Do we really need to pass the cache_root? Can it perhaps be set on the fsnode already?
		source_path = os.path.join(cache_root, fsnode.path.lstrip("/"))
		object_name = fsnode.path.lstrip("/")

		metadata = {
				"fs-mode": "%i" % fsnode.mode,
				"fs-uid": "%i" % fsnode.uid,
				"fs-gid": "%i" % fsnode.gid,
				"fs-mtime": "%f" % fsnode.mtime,
				"fs-atime": "%f" % fsnode.atime,
				"fs-ctime": "%f" % fsnode.ctime,
				"fs-nlink": "%i" % fsnode.nlink,
				"fs-size": "%i" % fsnode.size
			}

		if os.path.islink(source_path):
			metadata["fs-link-source"] = fsnode.link_source

		task = SwiftTask(command = "create_object", 
				args = {
					"object_name": object_name,
					"source_path": source_path,
					"metadata": metadata,
					"md5sum": md5sum
				})
		self.active_job_callbacks[task.job_id] = callback
		self.task_queue.put(task)

	def terminate_workers(self):
		def callback(success, error_message):
			pass
		for i in range(0,self.num_workers):
			task = SwiftTask(command = "shutdown", args = {})
			self.active_job_callbacks[task.job_id] = callback
			self.task_queue.put(task)

	def _response_thread_main(self):
		while True:
			try:
				response = self.response_queue.get()
				callback = self.active_job_callbacks[response.job_id]
				callback(response.success, response.error_message)
			except Exception, e:
				print e
			# TODO: from whatever is passed in the response, we need to be able to determine:
			#       a) did the request succeed
			#       b) execute the callback for the request
			#          > for this we'll likely need a dict so we can find the callback as you can't pass callbacks
			#            through a multiprocess queue


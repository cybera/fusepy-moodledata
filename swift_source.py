import pyrax
import os

class SwiftSource:
	def __init__(self, auth_url, username, password, tenant_id, region_name, source_bucket):
		pyrax.settings.set('identity_type', 'keystone')
		pyrax.set_setting("auth_endpoint", auth_url)
		pyrax.set_credentials(username=username, api_key=password, tenant_id=tenant_id)
		self.swift_client = pyrax.connect_to_cloudfiles(region_name)
		self.swift_mount = self.swift_client.get_container(source_bucket)

	def get_object(self, path):
		return self.swift_mount.get_object(path.lstrip("/"))

	def get_objects(self, path):
		return self.swift_mount.get_objects(prefix = path.lstrip("/"))

	def update_object(self, fsnode, cache_root):
		# TODO: Do we really need to pass the cache_root? Can it perhaps be set on the fsnode already?
		# TODO: This needs to be way more efficient. Large files won't be able to be read into a single string.
		# TODO: We'll probably want to use the "upload file" functionality of pyrax instead for actual files
		data = ""
		source_path = os.path.join(cache_root, fsnode.path.lstrip("/"))
		if os.path.isfile(source_path):
			with open (source_path, "r") as source_file:
				data = source_file.read()

		print "data: %s" % data

		obj = self.swift_mount.store_object(fsnode.path.lstrip("/"), data)

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

		obj.set_metadata(metadata)

	def move_object(self, old, new):
		obj = self.get_object(old)
		if obj:
			obj.move(obj.container, new.lstrip("/"))

		# This assumes we're moving within the same bucket

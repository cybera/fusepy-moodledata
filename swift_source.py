import pyrax

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
import ConfigParser, os, inspect

class Config:
	def __init__(self, section="main"):
		self.section = section
		self.parser = ConfigParser.ConfigParser()
		base_path = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
		local_config_path = os.path.join(base_path,'mount.cfg')

		for config_path in [ "/etc/moodledata-fuse/mount.cfg", local_config_path ]:
			if os.path.isfile(config_path):
				self.parser.readfp(open(config_path))

	def __getitem__(self,key):
		return self.parser.get(self.section, key)
import os

from threading import Lock

class File:
	def __init__(self, path, file_system):
		self.path = path
		self.file_system = file_system
		self.rwlock = Lock()

	def cache_path(self):
		return self.file_system.cache_path(self.path)

	def access(self, mode):
		if not os.access(self.cache_path(), mode):
			raise FuseOSError(EACCES)

	def chmod(self, mode):
		return os.chmod(self.cache_path(), mode)

	def chown(self, uid, gid):
		return os.chown(self.cache_path(), uid, gid)

	def create(self, mode):
		return os.open(self.cache_path(), os.O_WRONLY | os.O_CREAT, mode)

	def flush(self, fh):
		return os.fsync(fh)

	def fsync(self, datasync, fh):
		return os.fsync(fh)

	def getattr(self, fh=None):
		st = os.lstat(self.cache_path())
		return dict((key, getattr(st, key)) for key in ('st_atime', 'st_ctime',
			'st_gid', 'st_mode', 'st_mtime', 'st_nlink', 'st_size', 'st_uid'))

	def getxattr(self, name, position=0):
		return {}
	
	def link(self, source):
		return os.link(source, self.cache_path())

	def listxattr(path):
		return None

	def mkdir(self, mode):
		return os.mkdir(self.cache_path(), mode)

	def mknod(self, mode, dev):
		return os.mknod(self.cache_path(), mode, dev)

	def open(self, flags):
		return os.open(self.cache_path(), flags)

	def read(self, size, offset, fh):
		with self.rwlock:
			os.lseek(fh, offset, 0)
			return os.read(fh, size)

	def readdir(self, fh):
		return ['.', '..'] + os.listdir(self.cache_path())

	def readlink(self):
		return os.readlink(self.cache_path())

	def release(self, fh):
		return os.close(fh)

	def rename(self, new):
		return os.rename(self.cache_path(), self.cache_dir + new)

	def rmdir(self):
		return os.rmdir(self.cache_path())

	def statfs(self):
		stv = os.statvfs(self.cache_path())
		return dict((key, getattr(stv, key)) for key in ('f_bavail', 'f_bfree',
			'f_blocks', 'f_bsize', 'f_favail', 'f_ffree', 'f_files', 'f_flag',
			'f_frsize', 'f_namemax'))

	def symlink(self, source):
		return os.symlink(source, self.cache_path())

	def truncate(self, length, fh=None):
		with open(self.cache_path(), 'r+') as f:
			f.truncate(length)

	def unlink(self):
		return os.unlink(self.cache_path())

	def utimens(self, times=None):
		return os.utime(self.cache_path(), times)

	def write(self, data, offset, fh):
		with self.rwlock:
			os.lseek(fh, offset, 0)
			return os.write(fh, data)
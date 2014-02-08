import os

from threading import Lock
from sqlalchemy import Column, Integer, String, INT, REAL
from file_system import Session
from file_system import Base

class FSNode(Base):
	__tablename__ = "nodes"
	id = Column(Integer, primary_key=True)
	path = Column(String)
	name = Column(String)
	folder = Column(String)
	mode = Column(INT)
	uid = Column(INT)
	gid = Column(INT)
	mtime = Column(REAL)
	atime = Column(REAL)
	ctime = Column(REAL)
	nlink = Column(INT)
	size = Column(INT)

	def attr(self):
		return {
			'st_atime': self.atime,
			'st_ctime': self.ctime,
			'st_gid': self.gid, 
			'st_mode': self.mode, 
			'st_mtime': self.mtime, 
			'st_nlink': self.nlink, 
			'st_size': self.size,
			'st_uid': self.uid
		}

	# def __init__(self, path, file_system):
	# 	self.path = path
	# 	self.file_system = file_system
	# 	self.rwlock = Lock()

	# def cache_path(self):
	# 	return self.file_system.cache_path(self.path)

	# def chmod(self, mode):
	# 	return os.chmod(self.cache_path(), mode)

	# def chown(self, uid, gid):
	# 	return os.chown(self.cache_path(), uid, gid)

	# def getattr(self, fh=None):
	# 	st = os.lstat(self.cache_path())
	# 	return dict((key, getattr(st, key)) for key in ('st_atime', 'st_ctime',
	# 		'st_gid', 'st_mode', 'st_mtime', 'st_nlink', 'st_size', 'st_uid'))

	# def getxattr(self, name, position=0):
	# 	return ""

	# def listxattr(path):
	# 	return None

	# def utimens(self, times=None):
	# 	return os.utime(self.cache_path(), times)

	# def statfs(self):
	# 	stv = os.statvfs(self.cache_path())
	# 	return dict((key, getattr(stv, key)) for key in ('f_bavail', 'f_bfree',
	# 		'f_blocks', 'f_bsize', 'f_favail', 'f_ffree', 'f_files', 'f_flag',
	# 		'f_frsize', 'f_namemax'))

# class File(FSNode):
# 	def __init__(self, path, file_system):
# 		super(File, self).__init__(path, file_system)

# 	def flush(self, fh):
# 		return os.fsync(fh)

# 	def fsync(self, datasync, fh):
# 		return os.fsync(fh)

# 	def open(self, flags):
# 		return os.open(self.cache_path(), flags)

# 	def read(self, size, offset, fh):
# 		with self.rwlock:
# 			os.lseek(fh, offset, 0)
# 			return os.read(fh, size)

# 	def readlink(self):
# 		return os.readlink(self.cache_path())

# 	def release(self, fh):
# 		return os.close(fh)

# 	def truncate(self, length, fh=None):
# 		with open(self.cache_path(), 'r+') as f:
# 			f.truncate(length)

# 	def write(self, data, offset, fh):
# 		with self.rwlock:
# 			os.lseek(fh, offset, 0)
# 			return os.write(fh, data)

# class Directory(FSNode):
# 	def __init__(self, path, file_system):
# 		super(Directory, self).__init__(path, file_system)

# 	def __refresh_metadata__(self):
# 		self.children

# 	def children(self):
# 		return [row[0] for row in self.db.select("SELECT name FROM nodes WHERE path = ?", self.path.lstrip("/"))]

# 	def rmdir(self):
# 		return os.rmdir(self.cache_path())
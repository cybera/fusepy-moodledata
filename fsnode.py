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
	dirty = Column(INT)

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

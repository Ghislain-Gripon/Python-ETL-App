from abc import abstractmethod
from typing import LiteralString

from file_system.FolderStructure import FolderStructure


class DBServer:

	def __init__(self, _file_handler: FolderStructure):
		self.file_handler: FolderStructure = _file_handler

	@classmethod
	@abstractmethod
	def query(cls, query: LiteralString, **kwargs):
		pass

	def __enter__(self):
		pass

	def __exit__(self, exc_type, exc_value, exc_trace_back):
		pass

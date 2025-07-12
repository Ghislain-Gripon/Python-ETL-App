from typing import LiteralString

from src.file_system.FolderStructure import FolderStructure


class DBServer:

	def __init__(self, _file_handler: FolderStructure):
		self.file_handler: FolderStructure = _file_handler

	def query(self, query: LiteralString):
		pass

	def __enter__(self):
		pass

	def __exit__(self, exc_type, exc_value, exc_trace_back):
		pass

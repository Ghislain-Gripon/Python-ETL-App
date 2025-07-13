from pandas import DataFrame

from file_system.FolderStructure import FolderStructure

from pathlib import Path


class DataWriter:

	def __init__(self, file_handler: FolderStructure):
		self.file_handler = file_handler

	def write(self, file_path: Path, data: DataFrame, error_if_exists=True):
		pass

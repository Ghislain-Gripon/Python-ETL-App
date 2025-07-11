from pandas import DataFrame

from DataWriter import DataWriter
from FolderStructure import FolderStructure

from pathlib import Path


class DataWriterJSON(DataWriter):
	def __init__(self, file_handler: FolderStructure):
		super().__init__(file_handler)

	def write(self, file_path: Path | str, data: DataFrame):
		if self.file_handler.exists(file_path):
			data.to_json(file_path)
		else:
			raise FileExistsError

from pandas import DataFrame

from writer.DataWriter import DataWriter
from file_system.FolderStructure import FolderStructure

from pathlib import Path


class DataWriterCSV(DataWriter):
	def __init__(self, file_handler: FolderStructure):
		super().__init__(file_handler)

	def write(self, file_path: Path, data: DataFrame, error_if_exists=True):
		if self.file_handler.exists(file_path) is error_if_exists:
			data.to_csv(file_path)
		else:
			raise FileExistsError

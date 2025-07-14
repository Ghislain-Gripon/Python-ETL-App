from pandas import DataFrame

from writer.DataWriter import DataWriter
from file_system.FolderStructure import FolderStructure

from pathlib import Path


class DataWriterJSON(DataWriter):
	def __init__(self, file_handler: FolderStructure):
		super().__init__(file_handler)

	def write(self, file_path: Path, data: DataFrame, error_if_exists=True):
		if self.file_handler.exists(file_path) and error_if_exists:
			raise FileExistsError
		else:
			data.to_json(file_path, orient="records", date_format="iso", date_unit="s")

from pathlib import Path
from typing import TextIO

from pandas import DataFrame, read_csv

from loader.DataLoader import DataLoader


class DataLoaderCSV(DataLoader):

	@classmethod
	def as_dataframe(cls, filepath_or_buffer: str | Path | TextIO, encoding: str) -> DataFrame:
		return read_csv(filepath_or_buffer, encoding=encoding)

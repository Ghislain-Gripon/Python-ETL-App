from abc import abstractmethod
from pathlib import Path
from typing import TextIO

from pandas import DataFrame


class DataLoader:

	@classmethod
	@abstractmethod
	def as_dataframe(cls, filepath_or_buffer: str | Path | TextIO, encoding: str) -> DataFrame:
		pass

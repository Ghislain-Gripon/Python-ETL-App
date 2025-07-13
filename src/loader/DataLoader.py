from pathlib import Path
from typing import TextIO

from pandas import DataFrame


class DataLoader:

	def as_dataframe(self, filepath_or_buffer: str | Path | TextIO, encoding: str) -> DataFrame:
		pass

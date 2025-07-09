from pathlib import Path
from typing import TextIO

import pandas
from pandas import DataFrame

from src.DataLoader import DataLoader


class DataLoaderCSV(DataLoader):

    def as_dataframe(self, filepath_or_buffer: str | Path | TextIO, encoding: str) -> DataFrame:
        return pandas.read_csv(filepath_or_buffer, encoding=encoding)

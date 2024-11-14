from pathlib import Path

import pandas, json5
from pandas import DataFrame
from io import StringIO

from src.DataLoader import DataLoader

class DataLoaderJSON(DataLoader):

     def as_dataframe(self, filepath_or_buffer) -> DataFrame:
        if isinstance(filepath_or_buffer, Path | str):
            filepath_or_buffer = StringIO(str(filepath_or_buffer))
        data = json5.load(filepath_or_buffer)
        return pandas.DataFrame.from_records(data)
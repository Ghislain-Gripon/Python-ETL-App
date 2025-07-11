from pathlib import Path
from typing import TextIO

import pandas, json5
from pandas import DataFrame

from DataLoader import DataLoader


class DataLoaderJSON(DataLoader):

    def as_dataframe(self, filepath_or_buffer: str | Path | TextIO, encoding: str) -> DataFrame:
        if isinstance(filepath_or_buffer, Path | str):
            with open(filepath_or_buffer, 'r', encoding=encoding) as f:
                data = json5.loads(f.read())
        else:
            data = json5.load(filepath_or_buffer, encoding=encoding)

        return pandas.DataFrame.from_records(data)

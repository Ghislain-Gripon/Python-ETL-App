from pathlib import Path

import pandas, json5
from pandas import DataFrame

from src.DataLoader import DataLoader

class DataLoaderJSON(DataLoader):

    def as_dataframe(self, filepath_or_buffer) -> DataFrame:
        if isinstance(filepath_or_buffer, Path | str):
            with open(filepath_or_buffer, 'r') as f:
                data = json5.loads(f.read())
        else:
            data = json5.load(filepath_or_buffer, encoding="utf-8-sig")

        return pandas.DataFrame.from_records(data)

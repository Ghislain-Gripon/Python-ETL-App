import pandas
from pandas import DataFrame

from src.DataLoader import DataLoader

class DataLoaderJSON(DataLoader):

     def as_dataframe(self, filepath_or_buffer) -> DataFrame:
        return pandas.read_json(filepath_or_buffer)
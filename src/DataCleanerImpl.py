import pandas as pd
from pandas import DataFrame

from src.DataCleaner import DataCleaner
from src.Decorators import debug

class DataCleanerImpl(DataCleaner):

    @debug
    def clean_duplicates(self, df:DataFrame) -> DataFrame:
        df = df.drop_duplicates()
        return df

    @debug
    def clean_string(self, df:DataFrame, columns:list) -> DataFrame:
        df[columns] = df[columns].apply(lambda r: r.str.upper().str.strip())
        return df

    @debug
    def clean_numbers(self, df:DataFrame, columns:list) -> DataFrame:
        for column in columns:
            df[column] = pd.to_numeric(df[column], errors="coerce", downcast="integer")
        return df

    @debug
    def clean_date(self, df:DataFrame, columns:list) -> DataFrame:
        for column in columns:
            df[column] = pd.to_datetime(df[column], format="mixed", dayfirst=True)
        return df
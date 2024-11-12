from pandas import DataFrame


class DataCleaner:

    def clean_duplicates(self, df:DataFrame) -> DataFrame:
        pass

    def format(self, df:DataFrame, column_format:dict | None = None) -> DataFrame:
        pass

    def clean_string(self, df:DataFrame, columns:list | None = None) -> DataFrame:
        pass

    def clean_numbers(self, df:DataFrame, columns:list | None = None) -> DataFrame:
        pass

    def clean_date(self, df:DataFrame, columns:list | None = None) -> DataFrame:
        pass
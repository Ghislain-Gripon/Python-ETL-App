from pandas import DataFrame


class DataCleaner:

    def clean(self, df:DataFrame) -> DataFrame:
        return self.clean_string(self.clean_numbers(df))

    def fill(self, df:DataFrame, column_filling_vals:dict | None = None) -> DataFrame:
        pass

    def format(self, df:DataFrame, column_format:dict | None = None) -> DataFrame:
        pass

    def clean_string(self, df:DataFrame, columns:DataFrame.columns | None = None) -> DataFrame:
        pass

    def clean_numbers(self, df:DataFrame, columns:DataFrame.columns | None = None) -> DataFrame:
        pass
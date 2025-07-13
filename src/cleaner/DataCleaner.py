from abc import abstractmethod

from pandas import DataFrame


class DataCleaner:

    @classmethod
    @abstractmethod
    def clean_duplicates(cls, df:DataFrame) -> DataFrame:
        pass

    @classmethod
    @abstractmethod
    def clean_string(cls, df:DataFrame, columns: list | None = None) -> DataFrame:
        pass

    @classmethod
    @abstractmethod
    def clean_numbers(cls, df:DataFrame, columns: list | None = None) -> DataFrame:
        pass

    @classmethod
    @abstractmethod
    def clean_date(cls, df:DataFrame, columns: list | None = None) -> DataFrame:
        pass
from pathlib import Path

from pandas import DataFrame


class FolderStructure:

    def __init__(self, config_file_path = None):
        self.config_file_path = config_file_path
        self.config:dict = dict()
        self.file_directories = dict()

    def move(self, source: str | Path, target: str | Path):
        pass

    def load(self, file_path: str | Path):
        pass

    def read_yaml(self, file_stream) -> dict:
        pass

    def get_file_list(self, regex:str) -> list[Path]:
        pass

    def get_config(self, ) -> dict:
        """
        Returns the configuration dictionary.
        """
        pass

    def write(self, df:DataFrame, file_path:Path | str) -> Path | str:
        """
        :param df: data to write
        :param file_path: string, Path, local or cloud location
        :return:
        """
        pass
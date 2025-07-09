import logging
import logging.config
import re

from typing import TextIO

from pandas import DataFrame
from src.FolderStructure import FolderStructure
from pathlib import Path
from src.Decorators import debug
from yaml import safe_load

class FolderStructureLocal(FolderStructure):

    def __init__(self, _config_file_path:str | Path = None):

        config_file_path:Path = Path(_config_file_path) if _config_file_path is not None else Path("config/config.yaml")
        super().__init__(config_file_path)

        config_file_stream = self.load(Path(self.config_file_path))
        self.config: dict = self.read_yaml(config_file_stream)["execution_environment"]["local"]
        config_file_stream.close()

        logger_config_stream = self.load(Path(self.config["data_directory_path"]["config"]["directories"]["config"]) /
                                         self.config["data_directory_path"]["config"]["files"]["logger_config"])
        logging.config.dictConfig(self.read_yaml(logger_config_stream))
        logger_config_stream.close()

        for io_direction in ["input", "output"]:
            for directory_name in self.config["data_directory_path"]["data"][io_direction]["directories"]:
                full_directory: Path = (Path(self.config["data_directory_path"]["base_path"])
                / self.config["data_directory_path"]["data"][io_direction]["base_path"]
                / directory_name
                )
                full_directory.mkdir(parents=True, exist_ok=True)
                self.file_directories[directory_name] = full_directory


    @debug
    def move(self, source: str | Path, target: str | Path) -> Path:

        source, target = Path(source), Path(target)

        if target.is_file():
            raise FileExistsError(f"{target} already exists")

        return source.rename(target)

    @debug
    def load(self, file_path: str | Path) -> TextIO:
        path: Path = Path(file_path)
        if Path.is_file(path):
            return open(path, 'r', encoding=self.config["encoding"])
        else:
            logging.error(f"No file located at {file_path}")
            raise FileNotFoundError(f"There is no file at {path}")

    @debug
    def read_yaml(self, file_stream) -> dict:
        """
                Safe loads a yaml dictionary from an open file stream.
                """

        _file: dict = dict()

        try:
            _file: dict = safe_load(file_stream)

        # catch a yaml related error to inform user of problem with config file
        except FileNotFoundError:
            logging.error("YAML file at {} couldn't be decoded.".format(file_stream))
            raise FileNotFoundError("YAML file at {} couldn't be decoded.".format(file_stream))

        except PermissionError:
            logging.error("Can not access {}, permission denied.".format(file_stream))
            raise PermissionError("Can not access {}, permission denied.".format(file_stream))

        except:
            logging.error("File reading error.")
            raise "File reading error."

        return _file

    @debug
    def get_file_list(self, regex:str) -> list[Path]:
        return [file for file in self.file_directories["inbound"].iterdir() if re.search(regex, file.name) is not None]

    @debug
    def get_config(self, ) -> dict:
        return self.config

    @debug
    def write(self, df:DataFrame, file_path:Path | str) -> Path | str:
        df.to_csv(file_path, index=False, mode="a")
        return Path(file_path)
import logging
import logging.config
import pandas as pd

from pandas import DataFrame
from src.FolderStructure import FolderStructure
from pathlib import Path
from src.Decorators import debug
from yaml import safe_load

class FolderStructureLocal(FolderStructure):

    def __init__(self, config_file_path:str | Path = None):

        config_file_path:Path = Path(config_file_path) if config_file_path is not None else Path("config/config.yaml")
        super().__init__(config_file_path)

        config_file_stream = self.load(Path(self.config_file_path))
        self.config: dict = self.read_yaml(config_file_stream)["execution_environment"]["local"]
        config_file_stream.close()

        logger_config_stream = self.load(Path(self.config["data_directory_path"]["config"]["directories"]["config"]) /
                                         self.config["data_directory_path"]["config"]["files"]["logger_config"])
        logging.config.dictConfig(self.read_yaml(logger_config_stream))
        logger_config_stream.close()

    @debug
    def move(self, source: str | Path, target: str | Path) -> Path:

        source, target = Path(source), Path(target)

        if target.is_file():
            raise FileExistsError(f"{target} already exists")

        return source.rename(target)

    @debug
    def load(self, file_path: str | Path):
        _file = None
        path: Path = Path(file_path)
        if Path.is_file(path):
            _file = open(str(path), 'r')
        else:
            logging.warning("No file located at {}".format(file_path))
            raise FileNotFoundError("There is no file at {}".format(path))

        return _file

    @debug
    def read_yaml(self, file_stream) -> dict:
        """
                Safe loads a yaml dictionary from an open file stream.
                """

        _file: dict = dict()

        try:
            _file: dict = safe_load(file_stream)
            logging.info("YAML configuration file successfully read.")

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
    def get_config(self, ) -> dict:
        return self.config

    @debug
    def write(self, df:DataFrame, file_path:Path | str) -> Path | str:
        df.to_csv(file_path, index=False, mode="a")
        return Path(file_path)
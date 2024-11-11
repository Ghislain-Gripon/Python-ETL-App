import logging
from pathlib import Path
from pandas import DataFrame, concat

from src.DataCleaner import DataCleaner
from src.DataCleanerImpl import DataCleanerImpl
from src.DataLoader import DataLoader
from src.Decorators import debug
from src.Workflow import Workflow

class WorkflowImp(Workflow):

    @debug
    def run_flow(self, ):

        data_cleaner:DataCleaner = DataCleanerImpl()
        file_types:list[dict] = self.config["data_directory_path"]["data"]["file_type"]
        data_files:dict[str, list[dict[str, Path | str]]] = self._get_files_by_type(file_types)
        input_data:dict[str, DataFrame] = self._build_data_frames(data_files)



    @debug
    def _build_data_frames(self, data_files:dict[str, list[dict[str, Path | str]]]) -> dict[str, DataFrame]:
        input_data: dict[str, DataFrame] = dict()

        for filetype, data_file in data_files:
            input_data[filetype] = DataFrame()
            for _file in data_file:
                data_loader: DataLoader = self.get_data_loader_class(_file["extension"])
                try:
                    file_data_frame:DataFrame = data_loader.as_dataframe(_file["file_path"])
                    input_data[filetype] = concat((input_data[filetype], file_data_frame))
                except FileNotFoundError:
                    logging.error(f"File {_file["file_path"].name} was not found at {_file["file_path"]}")

                except PermissionError:
                    logging.error(f"Can not open file {_file["file_path"].name} at {_file["file_path"]}, permission error")

                except:
                    logging.error(f"Unkown error occured processing file {_file["file_path"].name} at {_file["file_path"]}")

        return input_data

    @debug
    def _get_files_by_type(self, file_types:list[dict]) -> dict[str, list[dict[str, Path | str]]]:
        data_files:dict[str, list[dict[str, Path | str]]] = dict()

        for file_type in file_types:
            work_dir:Path = self.file_handler.file_directories["work"]
            #Path.suffix returns the '.' at along with the extension, like '.py'
            data_files[file_type["type"]] = [ { "file_path":file.rename( work_dir / file.name ), "extension":str(file.suffix[1:]) }
                                              for file in self.file_handler.get_file_list(file_type["file_regex"]) ]
        return data_files
import logging
from pathlib import Path
from typing import TextIO

import numpy as np
from pandas import DataFrame, concat

from src.DataCleaner import DataCleaner
from src.DataCleanerImpl import DataCleanerImpl
from src.DataLoader import DataLoader
from src.Decorators import debug
from src.Graph import Graph
from src.GraphDrugs import GraphDrugs
from src.Workflow import Workflow

class WorkflowImp(Workflow):

    @debug
    def run_flow(self, ):

        data_cleaner:DataCleaner = DataCleanerImpl()

        file_types:list[dict] = self.config["data_directory_path"]["data"]["file_type"]
        data_files:dict[str, list[dict[str, Path | str]]] = self._get_files_by_type(file_types)
        input_data:dict[str, DataFrame] = self._build_data_frames(data_files)

        clean_input_data:dict[str, DataFrame] = self._clean_input_data(input_data, data_cleaner)

        relationship_graph:Graph = GraphDrugs(
            drugs_df=clean_input_data["drugs"],
            pubmed_and_clinical_trials_df=concat((clean_input_data["clinical_trials"], clean_input_data["pubmed"])))

        file_dirs: dict = self.file_handler.file_directories
        graph:dict = relationship_graph.get_graph()
        self.file_handler.write(graph["nodes"], file_dirs["done"] / "graph" / "nodes.csv")
        self.file_handler.write(graph["edges"], file_dirs["done"] / "graph" / "edges.csv")


    @debug
    def _build_data_frames(self, data_files:dict[str, list[dict[str, Path | str]]]) -> dict[str, DataFrame]:
        input_data: dict[str, DataFrame] = dict()

        for filetype, data_file in data_files.items():
            input_data[filetype] = DataFrame()
            for _file in data_file:
                data_loader: DataLoader = self.get_data_loader_class(_file["extension"])
                file_stream:TextIO | None = None
                try:
                    file_stream = self.file_handler.load(_file["file_path"])
                    file_data_frame:DataFrame = data_loader.as_dataframe(file_stream)
                    input_data[filetype] = concat((input_data[filetype], file_data_frame))

                except FileNotFoundError:
                    logging.error(f"File {_file["file_path"].name} was not found at {_file["file_path"]}")
                    self.file_handler.move(_file["file_path"], self.file_handler.file_directories["error"] / _file.name)

                except PermissionError:
                    logging.error(f"Can not open file {_file["file_path"].name} at {_file["file_path"]}, permission error")
                    self.file_handler.move(_file["file_path"], self.file_handler.file_directories["error"] / _file.name)

                except Exception:
                    logging.error(f"Unknown error occurred processing file {_file["file_path"].name} at {_file["file_path"]}")
                    self.file_handler.move(_file["file_path"], self.file_handler.file_directories["error"] / _file.name)

                else:
                    file_dirs:dict = self.file_handler.file_directories
                    self.file_handler.move(_file["file_path"], file_dirs["done"] / "processed_data" / _file.name)

                finally:
                    file_stream.close()

        return input_data

    @debug
    def _get_files_by_type(self, file_types:list[dict]) -> dict[str, list[dict[str, Path | str]]]:
        data_files:dict[str, list[dict[str, Path | str]]] = dict()

        for file_type in file_types:
            work_dir:Path = self.file_handler.file_directories["work"]
            #Path.suffix returns the '.' at along with the extension, like '.py'
            data_files[file_type["type"]] = [ { "file_path": self.file_handler.move(file, work_dir / file.name), "extension":str(file.suffix[1:]) }
                                              for file in self.file_handler.get_file_list(file_type["file_regex"]) ]
        return data_files

    @debug
    def _clean_input_data(self, input_data:dict[str, DataFrame], data_cleaner:DataCleaner) -> dict[str, DataFrame]:
        input_data["clinical_trials"] = self._clean_clinical_trials(input_data["clinical_trials"], data_cleaner)
        input_data["drugs"] = self._clean_drugs(input_data["drugs"], data_cleaner)
        input_data["pubmed"] = self._clean_pubmed(input_data["pubmed"], data_cleaner)
        return input_data

    @debug
    def _clean_clinical_trials(self, clinical_trials_data:DataFrame, data_cleaner:DataCleaner) -> DataFrame:
        clinical_trials_data.rename(columns={"scientific_title": "title"}, inplace=True)
        clinical_trials_data = data_cleaner.clean_string(clinical_trials_data, ["title","journal"])
        clinical_trials_data = data_cleaner.clean_date(clinical_trials_data, ["date"])
        clinical_trials_data.replace("", np.nan, inplace=True)
        clinical_trials_data.dropna(subset=["journal"], inplace=True)
        clinical_trials_data[["id", "date", "journal"]] = clinical_trials_data.groupby("title").transform(lambda x: x.loc[~x.isnull()].iloc[0])[["id", "date", "journal"]]
        clinical_trials_data.drop_duplicates(inplace=True)
        clinical_trials_data["type"] = "clinical_trials"
        return clinical_trials_data

    @debug
    def _clean_drugs(self, drugs_data: DataFrame, data_cleaner:DataCleaner) -> DataFrame:
        drugs_data = data_cleaner.clean_string(drugs_data, ["atccode", "drug"])
        drugs_data.replace("", np.nan, inplace=True)
        drugs_data.dropna(subset=["drug"], inplace=True)
        return drugs_data


    @debug
    def _clean_pubmed(self, pubmed_data: DataFrame, data_cleaner:DataCleaner) -> DataFrame:
        pubmed_data = data_cleaner.clean_string(pubmed_data, ["title", "journal"])
        pubmed_data = data_cleaner.clean_date(pubmed_data, ["date"])
        pubmed_data.replace("", np.nan, inplace=True)
        pubmed_data.dropna(subset=["journal"], inplace=True)
        pubmed_data[["id", "date", "journal"]] = pubmed_data.groupby("title").transform(lambda x: x.loc[~x.isnull()].iloc[0])[["id", "date", "journal"]]
        pubmed_data.drop_duplicates(inplace=True)
        pubmed_data["type"] = "pubmed"
        return pubmed_data



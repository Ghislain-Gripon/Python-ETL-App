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

class WorkflowImpl(Workflow):

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
        work_dir: Path = self.file_handler.file_directories["work"]

        for filetype, data_file in data_files.items():
            input_data[filetype] = DataFrame()
            for _file in data_file:
                data_loader: DataLoader = self.get_data_loader_class(_file["extension"])
                file_stream:TextIO | None = None
                file_path: Path | None = None
                try:

                    file_path = self.file_handler.move(_file["file_path"], work_dir / _file["file_path"].name)
                    _file["file_path"] = file_path
                    file_stream = self.file_handler.load(file_path)
                    file_data_frame:DataFrame = data_loader.as_dataframe(file_stream)
                    file_stream.close()

                    input_data[filetype] = concat((input_data[filetype], file_data_frame))

                except FileNotFoundError:
                    logging.error(f"File {file_path.name} was not found at {file_path}")
                    _file["file_path"] = self.file_handler.move(_file["file_path"], self.file_handler.file_directories["error"] / file_path.name)

                except PermissionError:
                    logging.error(f"Can not open file {file_path.name} at {file_path}, permission error")
                    _file["file_path"] = self.file_handler.move(_file["file_path"], self.file_handler.file_directories["error"] / file_path.name)

                except Exception:
                    logging.error(f"Unknown error occurred processing file {file_path.name} at {file_path}")
                    _file["file_path"] = self.file_handler.move(_file["file_path"], self.file_handler.file_directories["error"] / file_path.name)

                else:
                    file_dirs:dict = self.file_handler.file_directories
                    _file["file_path"] = self.file_handler.move(file_path, file_dirs["done"] / "processed_data" / file_path.name)

                finally:
                    try:
                        file_stream.close()
                    except BufferError:
                        pass

        return input_data

    @debug
    def _get_files_by_type(self, file_types:list[dict]) -> dict[str, list[dict[str, Path | str]]]:
        data_files:dict[str, list[dict[str, Path | str]]] = dict()

        for file_type in file_types:
            #Path.suffix returns the '.' at along with the extension, like '.py'
            data_files[file_type["type"]] = [ { "file_path": file, "extension":str(file.suffix[1:]) }
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
        clinical_trials_data_cleaned:DataFrame = self._clean_pubmed_or_clinical_trials(clinical_trials_data, data_cleaner)
        clinical_trials_data_cleaned["type"] = "clinical_trials"
        return clinical_trials_data_cleaned

    @debug
    def _clean_drugs(self, drugs_data: DataFrame, data_cleaner:DataCleaner) -> DataFrame:
        drugs_data = data_cleaner.clean_string(drugs_data, ["atccode", "drug"])
        drugs_data.replace("", np.nan, inplace=True)
        drugs_data.dropna(subset=["drug"], inplace=True)
        return drugs_data


    @debug
    def _clean_pubmed(self, pubmed_data: DataFrame, data_cleaner:DataCleaner) -> DataFrame:
        pubmed_data_fused:DataFrame = self._clean_pubmed_or_clinical_trials(pubmed_data, data_cleaner)
        pubmed_data_fused["type"] = "pubmed"
        return pubmed_data_fused

    @debug
    def _clean_pubmed_or_clinical_trials(self, pubmed_or_clinical_trials_data:DataFrame, data_cleaner:DataCleaner) -> DataFrame:
        pubmed_or_clinical_trials_data = data_cleaner.clean_string(pubmed_or_clinical_trials_data, ["title", "journal"])
        pubmed_or_clinical_trials_data = data_cleaner.clean_date(pubmed_or_clinical_trials_data, ["date"])
        pubmed_or_clinical_trials_data = data_cleaner.clean_numbers(pubmed_or_clinical_trials_data, ["id"])
        pubmed_or_clinical_trials_data = pubmed_or_clinical_trials_data.replace("", np.nan)

        pubmed_or_clinical_trials_data_fill_missing_by_title: DataFrame = pubmed_or_clinical_trials_data.copy(deep=True)
        pubmed_or_clinical_trials_data_fill_missing_by_title[["id", "date", "journal"]] = pubmed_or_clinical_trials_data_fill_missing_by_title.groupby("title").bfill()[["id", "date", "journal"]]

        pubmed_or_clinical_trials_data_fill_missing_by_title.dropna(subset=["journal", "title", "date"], inplace=True)

        pubmed_or_clinical_trials_data_fill_missing_by_id: DataFrame = pubmed_or_clinical_trials_data.copy(deep=True)
        pubmed_or_clinical_trials_data_fill_missing_by_id[["title", "date", "journal"]] = pubmed_or_clinical_trials_data_fill_missing_by_id.groupby("id").bfill()[["title", "date", "journal"]]

        pubmed_or_clinical_trials_data_fill_missing_by_id.dropna(subset=["journal", "title", "date"], inplace=True)

        pubmed_or_clinical_trials_data_fused: DataFrame = concat(
            (pubmed_or_clinical_trials_data_fill_missing_by_title, pubmed_or_clinical_trials_data_fill_missing_by_id))

        pubmed_or_clinical_trials_data_fused.drop_duplicates(inplace=True)
        pubmed_or_clinical_trials_data_fused["id"] = pubmed_or_clinical_trials_data_fused["id"].astype(np.int64)
        return pubmed_or_clinical_trials_data_fused



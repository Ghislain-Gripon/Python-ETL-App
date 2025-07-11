import logging
from pathlib import Path

import numpy as np
from pandas import DataFrame, concat
from pandas.errors import ParserError
from src.DataCleaner import DataCleaner
from src.DataCleanerImpl import DataCleanerImpl
from src.DataLoader import DataLoader
from src.DataWriterJSON import DataWriterJSON
from src.Decorators import debug
from src.Graph import Graph
from src.GraphDrugs import GraphDrugs
from src.Workflow import Workflow


class WorkflowImpl(Workflow):

	@debug
	def run_flow(self, ):
		file_types: list[dict[str, str]] = self.config["data_directory_path"]["data"]["file_type"]
		data_files: list[dict[str, Path | str]] = self._get_files_by_type(file_types)
		self._run_flow(data_files)

	@debug
	def _run_flow(self, data_files: list[dict[str, Path | str]]):

		try:
			input_data: dict[str, DataFrame] = self._build_data_frames(data_files)

			data_cleaner: DataCleaner = DataCleanerImpl()
			clean_input_data: dict[str, DataFrame] = self._clean_input_data(input_data, data_cleaner)

			relationship_graph: Graph = GraphDrugs(
				drugs_df=clean_input_data["drugs"],
				pubmed_and_clinical_trials_df=concat((clean_input_data["clinical_trials"], clean_input_data["pubmed"])))

			file_dirs: dict[str, Path] = self.file_handler.file_directories
			graph: dict[str, DataFrame] = relationship_graph.get_graph()
			json_writer: DataWriterJSON = DataWriterJSON(self.file_handler)

			json_writer.write(data=graph["nodes"], file_path=file_dirs["done"]/"graph"/"nodes.json")
			json_writer.write(data=graph["edges"], file_path=file_dirs["done"]/"graph"/"edges.json")

		except FileNotFoundError as e:
			for data_file in data_files:
				file_path = data_file["file_path_or_buffer"]
				logging.error(f"File {file_path.name} was not found at {file_path}. {e.args}")
				data_file["file_path_or_buffer"] = self.file_handler.move(
					data_file["file_path_or_buffer"],
					self.file_handler.file_directories["error"]/file_path.name)

		except PermissionError as e:
			for data_file in data_files:
				file_path = data_file["file_path_or_buffer"]
				logging.error(f"Can not open file {file_path.name} at {file_path}, permission error. {e.args}")
				data_file["file_path_or_buffer"] = self.file_handler.move(
					data_file["file_path_or_buffer"],
					self.file_handler.file_directories["error"]/file_path.name)

		except ParserError as e:
			for data_file in data_files:
				file_path = data_file["file_path_or_buffer"]
				logging.error(f"File {file_path.name} at {file_path} wasn't parsed correctly. {e.args}")
				data_file["file_path_or_buffer"] = self.file_handler.move(
					data_file["file_path_or_buffer"],
					self.file_handler.file_directories["error"]/file_path.name)

		except Exception as e:
			for data_file in data_files:
				file_path = data_file["file_path_or_buffer"]
				logging.error(f"{type(e)} error occurred processing file {file_path.name} at {file_path} with args: "
							  f"{e.args}")
				data_file["file_path_or_buffer"] = self.file_handler.move(
					data_file["file_path_or_buffer"],
					self.file_handler.file_directories["error"]/file_path.name)

	@debug
	def _build_data_frames(self, data_files: list[dict[str, Path | str]]) -> dict[str, DataFrame]:
		input_data: dict[str, DataFrame] = dict()
		file_dirs: dict = self.file_handler.file_directories

		for data_file in data_files:
			if data_file["type"] not in input_data:
				input_data[data_file["type"]] = DataFrame()

			data_loader: DataLoader = self.get_data_loader_class(data_file["extension"])

			data_file["file_path_or_buffer"] = self.file_handler.move(data_file["file_path_or_buffer"],
																	  file_dirs["work"]/data_file[
																		  "file_path_or_buffer"].name)

			with self.file_handler.load(data_file["file_path_or_buffer"]) as f:
				file_data_frame: DataFrame = data_loader.as_dataframe(f, self.config["encoding"])

			input_data[data_file["type"]] = concat((input_data[data_file["type"]], file_data_frame))

			data_file["file_path_or_buffer"] = self.file_handler.move(
				data_file["file_path_or_buffer"],
				file_dirs["done"]/"processed_data"/data_file["file_path_or_buffer"].name)

		return input_data

	@debug
	def _get_files_by_type(self, file_types: list[dict[str, str]]) -> list[dict[str, Path | str]]:
		data_files: list[dict[str, Path | str]] = []

		for file_type in file_types:
			# Path.suffix returns the '.' at along with the extension, like '.py'
			data_files += [
				{ "type": file_type["type"], "file_path_or_buffer": file_path, "extension": str(file_path.suffix[1:]) }
				for file_path in self.file_handler.get_file_list(file_type["file_regex"])]
		return data_files

	@debug
	def _clean_input_data(self, input_data: dict[str, DataFrame], data_cleaner: DataCleaner) -> dict[str, DataFrame]:
		input_data["clinical_trials"] = self._clean_clinical_trials(input_data["clinical_trials"], data_cleaner)
		input_data["drugs"] = self._clean_drugs(input_data["drugs"], data_cleaner)
		input_data["pubmed"] = self._clean_pubmed(input_data["pubmed"], data_cleaner)
		return input_data

	@debug
	def _clean_clinical_trials(self, clinical_trials_data: DataFrame, data_cleaner: DataCleaner) -> DataFrame:
		clinical_trials_data.rename(columns={ "scientific_title": "title" }, inplace=True)
		clinical_trials_data_cleaned: DataFrame = self._clean_pubmed_or_clinical_trials(clinical_trials_data,
																						data_cleaner)
		clinical_trials_data_cleaned["type"] = "clinical_trials"
		return clinical_trials_data_cleaned

	@debug
	def _clean_drugs(self, drugs_data: DataFrame, data_cleaner: DataCleaner) -> DataFrame:
		drugs_data = data_cleaner.clean_string(drugs_data, ["atccode", "drug"])
		drugs_data.replace("", np.nan, inplace=True)
		drugs_data.dropna(subset=["drug"], inplace=True)
		return drugs_data

	@debug
	def _clean_pubmed(self, pubmed_data: DataFrame, data_cleaner: DataCleaner) -> DataFrame:
		pubmed_data: DataFrame = self._clean_pubmed_or_clinical_trials(pubmed_data, data_cleaner)
		pubmed_data["type"] = "pubmed"
		return pubmed_data

	@debug
	def _clean_pubmed_or_clinical_trials(self, pubmed_or_clinical_trials_data: DataFrame,
										 data_cleaner: DataCleaner) -> DataFrame:
		pubmed_or_clinical_trials_data = data_cleaner.clean_string(pubmed_or_clinical_trials_data, ["title", "journal"])
		pubmed_or_clinical_trials_data = data_cleaner.clean_date(pubmed_or_clinical_trials_data, ["date"])
		pubmed_or_clinical_trials_data = pubmed_or_clinical_trials_data.replace("", np.nan)

		pubmed_or_clinical_trials_data_fill_missing_by_title: DataFrame = pubmed_or_clinical_trials_data.copy(deep=True)
		pubmed_or_clinical_trials_data_fill_missing_by_title[["id", "date", "journal"]] = \
			pubmed_or_clinical_trials_data_fill_missing_by_title.groupby("title").bfill()[["id", "date", "journal"]]

		pubmed_or_clinical_trials_data_fill_missing_by_title[["id", "date", "journal"]] = \
			pubmed_or_clinical_trials_data_fill_missing_by_title.groupby("title").ffill()[["id", "date", "journal"]]

		pubmed_or_clinical_trials_data_fill_missing_by_title.dropna(subset=["journal", "title", "date"], inplace=True)

		pubmed_or_clinical_trials_data_fill_missing_by_title.drop_duplicates(inplace=True)
		return pubmed_or_clinical_trials_data_fill_missing_by_title

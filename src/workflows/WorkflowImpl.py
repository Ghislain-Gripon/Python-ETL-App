import logging
from pathlib import Path

import numpy as np
from pandas import DataFrame, concat
from pandas.errors import ParserError
from cleaner.DataCleaner import DataCleaner
from cleaner.DataCleanerImpl import DataCleanerImpl
from loader.DataLoader import DataLoader
from writer.DataWriterJSON import DataWriterJSON
from Decorators import debug
from graph.Graph import Graph
from graph.GraphDrugs import GraphDrugs
from workflows.Workflow import Workflow


class WorkflowImpl(Workflow):

	@debug
	def run_flow(self, ) -> None:
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
				pubmed_and_clinical_trials_df=concat((clean_input_data["clinical_trials"], clean_input_data["pubmed"]))
			)

			file_dirs: dict[str, Path] = self.file_handler.file_directories
			graph: dict[str, DataFrame] = relationship_graph.get_graph()
			json_writer: DataWriterJSON = DataWriterJSON(self.file_handler)

			json_writer.write(data=graph["nodes"], file_path=file_dirs["done"]/"graph"/"nodes.json", error_if_exists=False)
			json_writer.write(data=graph["edges"], file_path=file_dirs["done"]/"graph"/"edges.json", error_if_exists=False)

		except FileNotFoundError as e:
			for data_file in data_files:
				file_path_1: Path = Path(data_file["file_path"])
				if self.file_handler.exists(file_path_1):
					data_file["file_path"] = self.file_handler.move(
						file_path_1,
						self.file_handler.file_directories["error"]/file_path_1.name
					)
				else:
					logging.error(f"File {file_path_1.name} was not found at {file_path_1}. {e.args}")
			raise e

		except PermissionError as e:
			for data_file in data_files:
				file_path_2: Path = Path(data_file["file_path"])
				logging.error(f"Can not open file {file_path_2.name} at {file_path_2}, permission error. {e.args}")
				data_file["file_path"] = self.file_handler.move(
					file_path_2,
					self.file_handler.file_directories["error"]/file_path_2.name
				)
			raise e


		except ParserError as e:
			for data_file in data_files:
				file_path_3: Path = Path(data_file["file_path"])
				logging.error(f"File {file_path_3.name} at {file_path_3} wasn't parsed correctly. {e.args}")
				data_file["file_path"] = self.file_handler.move(
					file_path_3,
					self.file_handler.file_directories["error"]/file_path_3.name
				)
			raise e


		except Exception as e:
			for data_file in data_files:
				file_path_4: Path = Path(data_file["file_path"])
				logging.error(
					f"{type(e)} error occurred processing file {file_path_4.name} at {file_path_4} with args: "
					f"{e.args}"
				)
				data_file["file_path"] = self.file_handler.move(
					file_path_4,
					self.file_handler.file_directories["error"]/file_path_4.name
				)
			raise e

	@debug
	def _build_data_frames(self, data_files: list[dict[str, Path | str]]) -> dict[str, DataFrame]:
		input_data: dict[str, DataFrame] = dict()
		file_dirs: dict[str, Path] = self.file_handler.file_directories

		for data_file in data_files:
			if data_file["type"] not in input_data:
				input_data[str(data_file["type"])] = DataFrame()

			data_loader: DataLoader = self.get_data_loader_class(data_file["extension"])

			data_file["file_path"] = self.file_handler.move(
				Path(data_file["file_path"]),
				file_dirs["work"]/Path(data_file["file_path"]).name
			)

			with self.file_handler.load(Path(data_file["file_path"])) as f:
				file_data_frame: DataFrame = data_loader.as_dataframe(f, self.config["encoding"])

			input_data[str(data_file["type"])] = concat((input_data[str(data_file["type"])], file_data_frame))

			data_file["file_path"] = self.file_handler.move(
				Path(data_file["file_path"]),
				file_dirs["done"]/"processed_data"/Path(data_file["file_path"]).name
			)

		return input_data

	@debug
	def _get_files_by_type(self, file_types: list[dict[str, str]]) -> list[dict[str, Path | str]]:
		data_files: list[dict[str, Path | str]] = []

		for file_type in file_types:
			# Path.suffix returns the '.' at along with the extension, like '.py'
			data_files += [
				{ "type": file_type["type"], "file_path": Path(file_path), "extension": str(
					file_path.suffix[
					1:]
				) }
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
		clinical_trials_data_cleaned: DataFrame = self._clean_pubmed_or_clinical_trials(
			clinical_trials_data,
			data_cleaner
		)
		clinical_trials_data_cleaned["type"] = "clinical_trials"
		return clinical_trials_data_cleaned

	@debug
	def _clean_drugs(self, drugs_data: DataFrame, data_cleaner: DataCleaner) -> DataFrame:
		drugs_data_clean = data_cleaner.clean_string(drugs_data, ["atccode", "drug"])
		drugs_data_clean.replace("", np.nan, inplace=True)
		drugs_data_clean.dropna(subset=["drug"], inplace=True)
		return drugs_data_clean

	@debug
	def _clean_pubmed(self, pubmed_data: DataFrame, data_cleaner: DataCleaner) -> DataFrame:
		pubmed_data_cleaned: DataFrame = self._clean_pubmed_or_clinical_trials(pubmed_data, data_cleaner)
		pubmed_data_cleaned["type"] = "pubmed"
		return pubmed_data_cleaned

	@debug
	def _clean_pubmed_or_clinical_trials(
			self, pubmed_or_clinical_trials_data: DataFrame,
			data_cleaner: DataCleaner
	) -> DataFrame:
		pubmed_or_clinical_trials_data_clean_string = data_cleaner.clean_string(
			pubmed_or_clinical_trials_data,
			["title", "journal"]
		)
		pubmed_or_clinical_trials_data_cleaned = data_cleaner.clean_date(
			pubmed_or_clinical_trials_data_clean_string,
			["date"]
		).replace("", np.nan)

		pubmed_or_clinical_trials_data_cleaned[["id", "date", "journal"]] = \
			pubmed_or_clinical_trials_data_cleaned.groupby("title").bfill()[["id", "date", "journal"]]

		pubmed_or_clinical_trials_data_cleaned[["id", "date", "journal"]] = \
			pubmed_or_clinical_trials_data_cleaned.groupby("title").ffill()[["id", "date", "journal"]]

		pubmed_or_clinical_trials_data_cleaned.dropna(subset=["journal", "title", "date"], inplace=True)

		pubmed_or_clinical_trials_data_cleaned.drop_duplicates(inplace=True)
		return pubmed_or_clinical_trials_data_cleaned

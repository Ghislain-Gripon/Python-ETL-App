import unittest
from pathlib import Path

import pandas as pd

from unittest import mock
from pandas import DataFrame, Timestamp
from pandas.testing import assert_frame_equal
from io import StringIO

from cleaner.DataCleanerImpl import DataCleanerImpl
from file_system.FolderStructure import FolderStructure
from file_system.FolderStructureLocal import FolderStructureLocal
from workflows.WorkflowImpl import WorkflowImpl


class WorkflowImplTest(unittest.TestCase):

	def test_clean_clinical_trials(self):
		file_handler_mock: FolderStructure = mock.Mock(spec=FolderStructure)
		file_handler_mock.get_config = mock.MagicMock(return_value=None)

		workflow: WorkflowImpl = WorkflowImpl(file_handler_mock)

		pubmed_and_clinical_trials_df: DataFrame = DataFrame(
			{
				"id": [1, 2, 2],
				"scientific_title": [
					"a 44-year-old man with erythema of the face diphenhydramine, neck, and chest, weakness, and palpitations",
					"an evaluation of benadryl, pyribenzamine, and other so-called diphenhydramine antihistaminic drugs in the treatment of allergy.",
					""],
				"date": [Timestamp("01/01/2019"), Timestamp("01/01/2019"), None],
				"journal": ["Journal of emergency emergencies", "Journal of emergency nursing",
							"Journal of emergency nursing"]
			}
		)

		cleaned_pubmed_and_clinical_trials_df: DataFrame = workflow._clean_clinical_trials(
			pubmed_and_clinical_trials_df, DataCleanerImpl()
		)

		val_pubmed_and_clinical_trials_df: DataFrame = DataFrame(
			{
				"id": [1.0, 2.0],
				"title": [
					"A 44-YEAR-OLD MAN WITH ERYTHEMA OF THE FACE DIPHENHYDRAMINE, NECK, AND CHEST, WEAKNESS, AND PALPITATIONS",
					"AN EVALUATION OF BENADRYL, PYRIBENZAMINE, AND OTHER SO-CALLED DIPHENHYDRAMINE ANTIHISTAMINIC DRUGS IN THE TREATMENT OF ALLERGY."],
				"date": [Timestamp("01/01/2019"), Timestamp("01/01/2019")],
				"journal": ["JOURNAL OF EMERGENCY EMERGENCIES", "JOURNAL OF EMERGENCY NURSING"]
			}
		)
		val_pubmed_and_clinical_trials_df["type"] = "clinical_trials"

		assert_frame_equal(cleaned_pubmed_and_clinical_trials_df, val_pubmed_and_clinical_trials_df)

	def test_clean_drugs(self):
		file_handler_mock: FolderStructure = mock.Mock(spec=FolderStructure)
		file_handler_mock.get_config = mock.Mock(return_value=None)

		workflow: WorkflowImpl = WorkflowImpl(file_handler_mock)

		drugs_data: str = """atccode,drug\nA04AD,DIPHENHYDRAMINE\nS03AA,PYRIBENZAMINE\nZERT3,"""

		drugs_df: DataFrame = pd.read_csv(StringIO(drugs_data)).dropna(subset="drug")

		cleaned_drugs_df: DataFrame = workflow._clean_drugs(drugs_df.copy(), DataCleanerImpl())

		assert_frame_equal(drugs_df, cleaned_drugs_df)

	def test_clean_pubmed(self):
		file_handler_mock: FolderStructure = mock.Mock(spec=FolderStructure)
		file_handler_mock.get_config = mock.Mock(return_value=None)

		workflow: WorkflowImpl = WorkflowImpl(file_handler_mock)

		pubmed_and_clinical_trials_data: str = """id,title,date,journal
                        1,"a 44-year-old man with erythema of the face diphenhydramine, neck, and chest, weakness, and palpitations",01/01/2019,"Journal of emergency emergencies"
                        2,"an evaluation of benadryl, pyribenzamine, and other so-called diphenhydramine antihistaminic drugs in the treatment of allergy.",01/01/2019,"Journal of emergency nursing"
                        """

		pubmed_and_clinical_trials_df: DataFrame = pd.read_csv(StringIO(pubmed_and_clinical_trials_data))

		cleaned_pubmed_and_clinical_trials_df: DataFrame = workflow._clean_pubmed(
			pubmed_and_clinical_trials_df, DataCleanerImpl()
		)

		val_pubmed_and_clinical_trials_df: DataFrame = DataFrame(
			{
				"id": [1, 2],
				"title": [
					"A 44-YEAR-OLD MAN WITH ERYTHEMA OF THE FACE DIPHENHYDRAMINE, NECK, AND CHEST, WEAKNESS, AND PALPITATIONS",
					"AN EVALUATION OF BENADRYL, PYRIBENZAMINE, AND OTHER SO-CALLED DIPHENHYDRAMINE ANTIHISTAMINIC DRUGS IN THE TREATMENT OF ALLERGY."],
				"date": [Timestamp("01/01/2019"), Timestamp("01/01/2019")],
				"journal": ["JOURNAL OF EMERGENCY EMERGENCIES", "JOURNAL OF EMERGENCY NURSING"]
			}
		)
		val_pubmed_and_clinical_trials_df["type"] = "pubmed"

		assert_frame_equal(
			cleaned_pubmed_and_clinical_trials_df, val_pubmed_and_clinical_trials_df, check_column_type=False
		)

	def test_get_files_by_type(self):
		file_handler: FolderStructure = FolderStructureLocal(Path("config/test_config.yaml"))
		workflow: WorkflowImpl = WorkflowImpl(file_handler)

		file_list: list[dict[str, Path | str]] = workflow._get_files_by_type(
			workflow.config["data_directory_path"]["data"]["file_type"]
		)

		val_file_list: list[dict[str, Path | str]] = [
			{ "type": "clinical_trials",
			  "file_path": Path("data/input/inbound/clinical_trials.csv"),
			  "extension": "csv" },
			{ "type": "drugs",
			  "file_path": Path("data/input/inbound/drugs.csv"),
			  "extension": "csv" },
			{ "type": "pubmed",
			  "file_path": Path("data/input/inbound/pubmed.csv"),
			  "extension": "csv" },
			{ "type": "pubmed",
			  "file_path": Path("data/input/inbound/pubmed.json"),
			  "extension": "json" }]

		self.assertListEqual(file_list, val_file_list)


def test_build_data_frames(self):

	file_handler: FolderStructure = FolderStructureLocal(Path("config/test_config.yaml"))
	workflow: WorkflowImpl = WorkflowImpl(file_handler)

	file_list: list[dict[str, Path | str]] = [
		{ "file_path": Path("data/input/inbound/clinical_trials.csv"), "extension": "csv", "type": "clinical_trials" },
		{ "file_path": Path("data/input/inbound/drugs.csv"), "extension": "csv", "type": "drugs" },
		{ "file_path": Path("data/input/inbound/pubmed.csv"), "extension": "csv", "type": "pubmed" },
		{ "file_path": Path("data/input/inbound/pubmed.json"), "extension": "json", "type": "pubmed" }
	]

	val_data_frames: dict[str, DataFrame] = dict()
	for value in file_list:
		if value.get("type") not in val_data_frames:
			val_data_frames[str(value.get("type"))] = DataFrame()

		val_data_frames[str(value.get("type"))] = pd.concat(
			(val_data_frames[str(value.get("type"))],
			 workflow.get_data_loader_class(value["extension"]).as_dataframe(
				 value["file_path"], encoding=str(file_handler.config.get("encoding"))
			 ))
		)

	data_frames: dict[str, DataFrame] = workflow._build_data_frames(file_list)

	for value in file_list:
		file_handler.move(
			Path(value["file_path"]), file_handler.file_directories["inbound"]/Path(value["file_path"]).name
		)

	for file_type in data_frames:
		data_frame_assert(data_frames, val_data_frames, file_type)


def data_frame_assert(data_frame: dict[str, DataFrame], val_data_frame: dict[str, DataFrame], file_type: str):
	assert_frame_equal(data_frame[file_type], val_data_frame[file_type])

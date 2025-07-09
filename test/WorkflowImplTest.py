import unittest
from pathlib import Path

import numpy as np
import pandas as pd

from unittest import mock
from pandas import DataFrame, Timestamp
from pandas.testing import assert_frame_equal
from io import StringIO

from src.DataCleanerImpl import DataCleanerImpl
from src.FolderStructure import FolderStructure
from src.FolderStructureLocal import FolderStructureLocal
from src.WorkflowImpl import WorkflowImpl


class WorkflowImplTest(unittest.TestCase):

    def test_clean_clinical_trials(self):

        file_handler_mock: FolderStructure = mock.Mock(spec=FolderStructure)
        file_handler_mock.get_config = mock.Mock(return_value=None)

        workflow: WorkflowImpl = WorkflowImpl(file_handler_mock)

        pubmed_and_clinical_trials_df: DataFrame = DataFrame({
            "id": [1, 2, 2],
            "scientific_title": [
                "a 44-year-old man with erythema of the face diphenhydramine, neck, and chest, weakness, and palpitations",
                "an evaluation of benadryl, pyribenzamine, and other so-called diphenhydramine antihistaminic drugs in the treatment of allergy.",
                ""],
            "date": [Timestamp("01/01/2019"), Timestamp("01/01/2019"), None],
            "journal": ["Journal of emergency emergencies", "Journal of emergency nursing",
                        "Journal of emergency nursing"]
            })

        cleaned_pubmed_and_clinical_trials_df: DataFrame = workflow._clean_clinical_trials(pubmed_and_clinical_trials_df, DataCleanerImpl())

        val_pubmed_and_clinical_trials_df: DataFrame = DataFrame({
            "id": [1, 2],
            "title": [
                "A 44-YEAR-OLD MAN WITH ERYTHEMA OF THE FACE DIPHENHYDRAMINE, NECK, AND CHEST, WEAKNESS, AND PALPITATIONS",
                "AN EVALUATION OF BENADRYL, PYRIBENZAMINE, AND OTHER SO-CALLED DIPHENHYDRAMINE ANTIHISTAMINIC DRUGS IN THE TREATMENT OF ALLERGY."],
            "date": [Timestamp("01/01/2019"), Timestamp("01/01/2019")],
            "journal": ["JOURNAL OF EMERGENCY EMERGENCIES", "JOURNAL OF EMERGENCY NURSING"]
            })
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
            pubmed_and_clinical_trials_df, DataCleanerImpl())

        val_pubmed_and_clinical_trials_df: DataFrame = DataFrame({
            "id": [1, 2],
            "title": [
                "A 44-YEAR-OLD MAN WITH ERYTHEMA OF THE FACE DIPHENHYDRAMINE, NECK, AND CHEST, WEAKNESS, AND PALPITATIONS",
                "AN EVALUATION OF BENADRYL, PYRIBENZAMINE, AND OTHER SO-CALLED DIPHENHYDRAMINE ANTIHISTAMINIC DRUGS IN THE TREATMENT OF ALLERGY."],
            "date": [Timestamp("01/01/2019"), Timestamp("01/01/2019")],
            "journal": ["JOURNAL OF EMERGENCY EMERGENCIES", "JOURNAL OF EMERGENCY NURSING"]
            })
        val_pubmed_and_clinical_trials_df["type"] = "pubmed"

        assert_frame_equal(cleaned_pubmed_and_clinical_trials_df, val_pubmed_and_clinical_trials_df, check_column_type=False)

    def test_get_files_by_type(self):

        file_handler: FolderStructure = FolderStructureLocal("config/config.yaml")
        workflow: WorkflowImpl = WorkflowImpl(file_handler)

        file_dict: dict[str, list[dict[str, Path | str]]] = workflow._get_files_by_type(
            workflow.config["data_directory_path"]["data"]["file_type"])

        val_file_dict: dict[str, list[dict[str, Path | str]]] = {
            "clinical_trials": [{ "file_path": Path("data/input/inbound/clinical_trials.csv"), "extension": "csv" }],
            "drugs": [{ "file_path": Path("data/input/inbound/drugs.csv"), "extension": "csv" }],
            "pubmed": [{ "file_path": Path("data/input/inbound/pubmed.csv"), "extension": "csv" },
                       { "file_path": Path("data/input/inbound/pubmed.json"), "extension": "json" }]
            }

        self.assertDictEqual(file_dict, val_file_dict)

    def test_build_data_frames(self):

        file_handler: FolderStructure = FolderStructureLocal("config/config.yaml")
        workflow: WorkflowImpl = WorkflowImpl(file_handler)

        file_dict: dict[str, list[dict[str, Path | str]]] = {
            "clinical_trials": [{ "file_path": Path("data/input/inbound/clinical_trials.csv"), "extension": "csv" }],
            "drugs": [{ "file_path": Path("data/input/inbound/drugs.csv"), "extension": "csv" }],
            "pubmed": [{ "file_path": Path("data/input/inbound/pubmed.csv"), "extension": "csv" },
                       { "file_path": Path("data/input/inbound/pubmed.json"), "extension": "json" }]
            }

        val_data_frames: dict[str, DataFrame] = dict()
        for file_type, value in file_dict.items():
            val_data_frames[file_type] = DataFrame()
            for entry in value:
                val_data_frames[file_type] = pd.concat((val_data_frames[file_type],
                                                        workflow.get_data_loader_class(entry["extension"]).as_dataframe(
                                                            entry["file_path"])))

        data_frames: dict[str, DataFrame] = workflow._build_data_frames(file_dict)

        for _, value in file_dict.items():
            for entry in value:
                file_handler.move(
                    entry["file_path"], file_handler.file_directories["inbound"] / entry["file_path"].name)

        for file_type in data_frames:
            self.data_frame_assert(data_frames, val_data_frames, file_type)

    def data_frame_assert(self, data_frame: dict[str, DataFrame], val_data_frame: dict[str, DataFrame], file_type: str):
        assert_frame_equal(data_frame[file_type], val_data_frame[file_type])

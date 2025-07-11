import unittest
import pandas as pd

from pandas import DataFrame
from pandas.testing import assert_frame_equal
from io import StringIO

from src.GraphDrugs import GraphDrugs

class GraphDrugsTest(unittest.TestCase):

    def test_build_graph_edges(self):
        drugs_data:str = """atccode,drug
        A04AD,DIPHENHYDRAMINE
        S03AA,PYRIBENZAMINE
        """

        drugs_df:DataFrame = pd.read_csv(StringIO(drugs_data))

        pubmed_and_clinical_trials_data:str = """id,title,date,journal,type
        1,"A 44-YEAR-OLD MAN WITH ERYTHEMA OF THE FACE DIPHENHYDRAMINE, NECK, AND CHEST, WEAKNESS, AND PALPITATIONS",01/01/2019,"Journal of emergency emergencies","pubmed"
        2,"AN EVALUATION OF BENADRYL, PYRIBENZAMINE, AND OTHER SO-CALLED DIPHENHYDRAMINE ANTIHISTAMINIC DRUGS IN THE TREATMENT OF ALLERGY.",01/01/2019,"Journal of emergency nursing","clinical_trials"
        """

        pubmed_and_clinical_trials_df:DataFrame = pd.read_csv(StringIO(pubmed_and_clinical_trials_data))

        graph:GraphDrugs = GraphDrugs(drugs_df, pubmed_and_clinical_trials_df)

        edges_df:DataFrame = DataFrame({
            "source": ["DIPHENHYDRAMINE",
                       "A 44-YEAR-OLD MAN WITH ERYTHEMA OF THE FACE DIPHENHYDRAMINE, NECK, AND CHEST, WEAKNESS, AND PALPITATIONS",
                       "DIPHENHYDRAMINE",
                       "AN EVALUATION OF BENADRYL, PYRIBENZAMINE, AND OTHER SO-CALLED DIPHENHYDRAMINE ANTIHISTAMINIC DRUGS IN THE TREATMENT OF ALLERGY.",
                       "PYRIBENZAMINE",
                       "AN EVALUATION OF BENADRYL, PYRIBENZAMINE, AND OTHER SO-CALLED DIPHENHYDRAMINE ANTIHISTAMINIC DRUGS IN THE TREATMENT OF ALLERGY."],
            "target": ["A 44-YEAR-OLD MAN WITH ERYTHEMA OF THE FACE DIPHENHYDRAMINE, NECK, AND CHEST, WEAKNESS, AND PALPITATIONS",
                       "Journal of emergency emergencies",
                       "AN EVALUATION OF BENADRYL, PYRIBENZAMINE, AND OTHER SO-CALLED DIPHENHYDRAMINE ANTIHISTAMINIC DRUGS IN THE TREATMENT OF ALLERGY.",
                       "Journal of emergency nursing",
                       "AN EVALUATION OF BENADRYL, PYRIBENZAMINE, AND OTHER SO-CALLED DIPHENHYDRAMINE ANTIHISTAMINIC DRUGS IN THE TREATMENT OF ALLERGY.",
                       "Journal of emergency nursing"],
            "date": ["01/01/2019", "01/01/2019", "01/01/2019", "01/01/2019", "01/01/2019", "01/01/2019"]
        })

        assert_frame_equal(graph.get_graph()["edges"], edges_df)

    def test_build_graph_nodes(self):

        drugs_data:str = """atccode,drug
        A04AD,DIPHENHYDRAMINE
        S03AA,PYRIBENZAMINE
        """

        drugs_df:DataFrame = pd.read_csv(StringIO(drugs_data))

        pubmed_and_clinical_trials_data:str = """id,title,date,journal,type
        1,"A 44-YEAR-OLD MAN WITH ERYTHEMA OF THE FACE DIPHENHYDRAMINE, NECK, AND CHEST, WEAKNESS, AND PALPITATIONS",01/01/2019,"Journal of emergency emergencies","pubmed"
        2,"AN EVALUATION OF BENADRYL, PYRIBENZAMINE, AND OTHER SO-CALLED DIPHENHYDRAMINE ANTIHISTAMINIC DRUGS IN THE TREATMENT OF ALLERGY.",01/01/2019,"Journal of emergency nursing","clinical_trials"
        """

        pubmed_and_clinical_trials_df:DataFrame = pd.read_csv(StringIO(pubmed_and_clinical_trials_data))

        graph:GraphDrugs = GraphDrugs(drugs_df, pubmed_and_clinical_trials_df)

        nodes_df:DataFrame = DataFrame({
            "type": ["drug", "pubmed", "journal","clinical_trials","journal","drug","clinical_trials","journal"],
            "data": ["DIPHENHYDRAMINE",
                     "A 44-YEAR-OLD MAN WITH ERYTHEMA OF THE FACE DIPHENHYDRAMINE, NECK, AND CHEST, WEAKNESS, AND PALPITATIONS",
                     "Journal of emergency emergencies",
                     "AN EVALUATION OF BENADRYL, PYRIBENZAMINE, AND OTHER SO-CALLED DIPHENHYDRAMINE ANTIHISTAMINIC DRUGS IN THE TREATMENT OF ALLERGY.",
                     "Journal of emergency nursing",
                     "PYRIBENZAMINE",
                     "AN EVALUATION OF BENADRYL, PYRIBENZAMINE, AND OTHER SO-CALLED DIPHENHYDRAMINE ANTIHISTAMINIC DRUGS IN THE TREATMENT OF ALLERGY.",
                     "Journal of emergency nursing"
                     ]
        })
        print(graph.get_graph()["nodes"]["data"])
        assert_frame_equal(graph.get_graph()["nodes"], nodes_df)
import pandas
from pandas import DataFrame
from src.Graph import Graph

class GraphDrugs(Graph):

    def __init__(self, drugs_df: DataFrame, pubmed_trials_df: DataFrame):
        super().__init__()
        self._build_graph(drugs_df, pubmed_trials_df)

    def _build_graph(self, drugs_df: DataFrame, pubmed_trials_df: DataFrame):
        edges_df: DataFrame = DataFrame()
        for _, drug in drugs_df.itertuples():
            drug_in_pubmed_trials_df: DataFrame = pubmed_trials_df.iloc[drug["drug"] in pubmed_trials_df["title"]]
            drug_in_pubmed_trials_df["drug"] = drug["drug"]
            edges_df = pandas.concat([edges_df, drug_in_pubmed_trials_df],
                                     ignore_index=True).drop_duplicates().reset_index()

        self.graph = edges_df

    def get_graph(self):
        return self.graph


import pandas as pd
from pandas import DataFrame
from src.Graph import Graph

class GraphDrugs(Graph):

    def __init__(self, drugs_df: DataFrame, pubmed_and_clinical_trials_df: DataFrame):
        super().__init__()
        self._build_graph(drugs_df, pubmed_and_clinical_trials_df)

    def _build_graph(self, drugs_df: DataFrame, pubmed_and_clinical_trials_df: DataFrame):
        nodes:list[dict] = []
        edges:list[dict] = []

        for _, drug in drugs_df.itertuples():
            nodes.append({"type": "drug", "data": drug["drug"]})
            drug_in_pubmed_trials_df: DataFrame = pubmed_and_clinical_trials_df.iloc[drug["drug"] in pubmed_and_clinical_trials_df["title"]]
            for _, mention in drug_in_pubmed_trials_df.itertuples():
                nodes.append({"type": mention["type"], "data": drug_in_pubmed_trials_df["title"]})
                nodes.append({"type": "journal", "data": drug_in_pubmed_trials_df["journal"]})
                edges.append({"source": drug["drug"], "target": drug_in_pubmed_trials_df["title"], "date": drug_in_pubmed_trials_df["date"]})
                edges.append({"source": drug_in_pubmed_trials_df["title"], "target": drug_in_pubmed_trials_df["journal"], "date": drug_in_pubmed_trials_df["date"]})

        self.graph["nodes"] = pd.DataFrame.from_records(nodes)
        self.graph["edges"] = pd.DataFrame.from_records(edges)

    def get_graph(self):
        return self.graph


from operator import index

import pandas as pd
from pandas import DataFrame
from src.Graph import Graph


class GraphDrugs(Graph):

	def __init__(self, drugs_df: DataFrame, pubmed_and_clinical_trials_df: DataFrame):
		super().__init__()
		self._build_graph(drugs_df, pubmed_and_clinical_trials_df)

	def _build_graph(self, drugs_df: DataFrame, pubmed_and_clinical_trials_df: DataFrame):
		nodes: list[dict] = []
		edges: list[dict] = []

		for _, drug in drugs_df.iterrows():
			nodes.append({ "type": "drug", "data": drug["drug"] })
			drug_in_pubmed_trials_df: DataFrame = pubmed_and_clinical_trials_df[
				pubmed_and_clinical_trials_df["title"].str.contains(drug["drug"])]
			for _, mention in drug_in_pubmed_trials_df.iterrows():
				nodes.append({ "type": mention["type"], "data": mention["title"] })
				nodes.append({ "type": "journal", "data": mention["journal"] })
				edges.append({ "source": drug["drug"], "source_type": "drug", "target": mention["title"],
							   "target_type": mention["type"], "date": mention["date"] })
				edges.append({ "source": mention["title"], "source_type": mention["type"], "target": mention["journal"],
							   "target_type": "journal", "date": mention["date"] })

		self.graph["nodes"] = pd.DataFrame.from_records(nodes)
		self.graph["edges"] = pd.DataFrame.from_records(edges)

	def get_graph(self):
		return self.graph

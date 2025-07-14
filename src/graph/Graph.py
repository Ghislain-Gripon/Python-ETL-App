from pandas import DataFrame


class Graph:

	def __init__(self):
		self.graph: dict[str, DataFrame] = {
			"nodes": DataFrame(),
			"edges": DataFrame()
		}

	def get_graph(self) -> dict[str, DataFrame]:
		return self.graph

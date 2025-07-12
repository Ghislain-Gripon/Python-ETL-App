from typing import LiteralString

from src.database.Neo4jDBServer import Neo4jDBServer
from src.file_system.FolderStructure import FolderStructure
from src.workflows.Workflow import Workflow


class Neo4jWorkflow(Workflow):
	def __init__(self, _file_handler: FolderStructure):
		super().__init__(_file_handler)

	def run_flow(self, ):
		with self.file_handler.load("ressources/neo4j/setup_graph.cypher") as f:
			setup_neo4j_query: LiteralString = f.read()

		with Neo4jDBServer(self.file_handler) as driver:
			driver.query(setup_neo4j_query)


	def question_4_1(self):

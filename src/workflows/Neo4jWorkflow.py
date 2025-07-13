from typing import LiteralString
import pandas
from neo4j import Result
from pprint import pprint

from src.database.Neo4jDBServer import Neo4jDBServer
from src.file_system.FolderStructure import FolderStructure
from src.workflows.Workflow import Workflow


def question_4_2(driver: Neo4jDBServer, drug: str):
	result: Result = driver.query(
		"""
		MATCH (j:Journal)-[:MENTION]->(d1:Drugs) WHERE d1.name = $drug
		MATCH (j)-[:MENTION]->(d:Drugs)-[:REFERENCE]->(:Pubmed) WHERE NOT (d)-[:REFERENCE]->(:Clinical_Trials) AND d.name <> d1.name
		WITH d.name as Name
		RETURN DISTINCT Name
		""",
		drug=drug
	)
	df: pandas.DataFrame = result.to_df()
	summary = result.consume()
	pprint(f"The query '{summary.query}' returned")
	pprint(df.to_clipboard)


def question_4_1(driver: Neo4jDBServer):
	result: Result = driver.query(
		"""
		MATCH (j:Journal)-[r:MENTION]->(:Drugs)
		WITH j.name AS Journal, count(r) AS drug_mentionned
		ORDER BY drug_mentionned DESC
		RETURN Journal
		LIMIT 1
		"""
	)
	df: pandas.DataFrame = result.to_df()
	summary = result.consume()
	pprint(f"The query '{summary.query}' returned")
	pprint(df.to_clipboard)


class Neo4jWorkflow(Workflow):
	def __init__(self, _file_handler: FolderStructure):
		super().__init__(_file_handler)

	def run_flow(self, ):
		with self.file_handler.load("ressources/neo4j/setup_graph.cypher") as f:
			setup_neo4j_query: LiteralString = f.read()

		with Neo4jDBServer(self.file_handler) as driver:
			driver.query(setup_neo4j_query)
			question_4_1(driver)
			question_4_2(driver, "TETRACYCLINE")

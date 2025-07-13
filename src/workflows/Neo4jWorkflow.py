from pathlib import Path
from typing import LiteralString
from neo4j import Result

from Decorators import debug
from database.Neo4jDBServer import Neo4jDBServer
from file_system.FolderStructure import FolderStructure
from workflows.Workflow import Workflow


@debug
def question_4_1(driver: Neo4jDBServer):
	result: Result = driver.query(
		"""
		MATCH (j:Journal)-[:MENTION]->(d:Drugs)
		WITH j.name AS Journal, count(DISTINCT d) AS drug_mentionned
		
		WITH collect({Journal: Journal, count: drug_mentionned}) AS journal_counts
		WITH journal_counts, reduce(maxVal = 0, jc IN journal_counts | 
			 CASE WHEN jc.count > maxVal THEN jc.count ELSE maxVal END) AS max_count
		
		UNWIND journal_counts AS jc
		WITH jc
		WHERE jc.count = max_count
		RETURN jc.Journal AS Journal
		"""
	)
	return result


@debug
def question_4_2(driver: Neo4jDBServer, drug: str):
	result: Result = driver.query(
		"""
		MATCH (j:Journal)-[:MENTION]->(d1:Drugs) WHERE d1.name = $drug  
		MATCH (j)-[:MENTION]->(d:Drugs)-[:REFERENCE]->(:Pubmed) 
		WHERE NOT (d)-[:REFERENCE]->(:Clinical_Trials) AND d.name <> d1.name  
		WITH d.name as Name  
		RETURN DISTINCT Name
		""",
		parameters={ "drug": drug }
	)
	return result


class Neo4jWorkflow(Workflow):
	def __init__(self, _file_handler: FolderStructure):
		super().__init__(_file_handler)

	def run_flow(self, ):
		with self.file_handler.load(Path("data/ressources/neo4j/setup_graph.cypher")) as f:
			setup_neo4j_queries: LiteralString = f.read()

		setup_neo4j = setup_neo4j_queries.split(";")

		with Neo4jDBServer(self.file_handler) as driver:
			for query in setup_neo4j:
				driver.query(query)

		with Neo4jDBServer(self.file_handler) as driver:
			question_4_1(driver)
			question_4_2(driver, "TETRACYCLINE")

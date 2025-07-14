import logging
import os
from pathlib import Path
from typing import LiteralString, Tuple

from Decorators import debug
from database.DBServer import DBServer
from neo4j import GraphDatabase, Driver

from file_system.FolderStructure import FolderStructure

class Neo4jDBServer(DBServer):

	def __init__(self, _file_handler: FolderStructure):
		super().__init__(_file_handler)

	@debug
	def query(self, query: LiteralString, parameters=None):
		if parameters is None:
			parameters = dict()
		return self.driver.execute_query(query, database_="neo4j", parameters_=parameters)

	def __enter__(self):
		neo4j_env_vars: dict[str, str] = self.file_handler.config["database"]["neo4j"]
		uri: str = os.environ[neo4j_env_vars["uri"]]
		auth_file: Path = Path(os.environ[neo4j_env_vars["auth_file"]])

		with self.file_handler.load(auth_file) as f:
			credentials: list[str] = f.read().split("/")

		auth: Tuple[str, str] = (credentials[0], credentials[1])

		self.driver: Driver = GraphDatabase.driver(
			uri=uri, auth=auth, connection_timeout=60.0,
			connection_acquisition_timeout=90.0,
			keep_alive=True
		)
		self.driver.verify_connectivity()
		return self

	def __exit__(self, exc_type, exc_value, exc_trace_back):
		self.driver.close()
		return True

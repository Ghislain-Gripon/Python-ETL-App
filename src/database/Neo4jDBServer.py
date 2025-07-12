import os
from typing import LiteralString

from src.Decorators import debug
from src.database.DBServer import DBServer
from neo4j import GraphDatabase, Driver, Query

from src.file_system.FolderStructure import FolderStructure


class Neo4jDBServer(DBServer):

	def __init__(self, _file_handler: FolderStructure):
		super().__init__(_file_handler)
		self.driver: Driver = None

	@debug
	def query(self, query: LiteralString):
		return self.driver.execute_query(query)

	def __enter__(self):
		neo4j_env_vars: dict[str, str] = self.file_handler.config["database"].get("neo4j")
		uri, auth_file = (os.getenv(neo4j_env_vars.get("uri")), os.getenv(neo4j_env_vars.get("auth_file")))

		with self.file_handler.load(auth_file) as f:
			credentials: list[str] = f.read().split("/")

		auth: (str, str) = (credentials[0], credentials[1])

		self.driver: Driver = GraphDatabase.driver(
			uri, auth=auth, connection_timeout=60.0,
			connection_acquisition_timeout=90.0
			)
		self.driver.verify_connectivity()
		return self

	def __exit__(self, exc_type, exc_value, exc_trace_back):
		self.driver.close()
		return True

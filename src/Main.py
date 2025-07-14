import os
import sys
from pathlib import Path

from file_system.FolderStructure import FolderStructure
from file_system.FolderStructureLocal import FolderStructureLocal
from workflows.WorkflowImpl import WorkflowImpl
from workflows.Neo4jWorkflow import Neo4jWorkflow

if __name__ == "__main__":

	if len(sys.argv) == 1:
		config_path = Path("data/config/config.yaml")
	else:
		config_path = Path(sys.argv[1])

	file_handler: FolderStructure = FolderStructureLocal(config_path)
	WorkflowImpl(file_handler).run_flow()
	neo4j_uri = file_handler.config["database"]["neo4j"].get("uri")
	if neo4j_uri is not None and os.getenv(neo4j_uri) is not None:
		Neo4jWorkflow(file_handler).run_flow()

import sys
from pathlib import Path

from src.file_system.FolderStructure import FolderStructure
from src.file_system.FolderStructureLocal import FolderStructureLocal
from src.workflows.WorkflowImpl import WorkflowImpl
from src.workflows.Neo4jWorkflow import Neo4jWorkflow

if __name__ == "__main__":

	if len(sys.argv) == 1:
		config_path = Path("config/config.yaml")
	else:
		config_path = Path(sys.argv[1])

	file_handler: FolderStructure = FolderStructureLocal(config_path)
	WorkflowImpl(file_handler).run_flow()
	Neo4jWorkflow(file_handler).run_flow()

import sys
from pathlib import Path

from FolderStructure import FolderStructure
from FolderStructureLocal import FolderStructureLocal
from WorkflowImpl import WorkflowImpl

if __name__ == "__main__":

    if len(sys.argv) == 1:
        config_path = Path("../config/config.yaml")
    else:
        config_path = Path(sys.argv[1])

    file_handler: FolderStructure = FolderStructureLocal(config_path)
    WorkflowImpl(file_handler).run_flow()

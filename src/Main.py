from src.FolderStructure import FolderStructure
from src.FolderStructureLocal import FolderStructureLocal
from src.WorkflowImpl import WorkflowImpl

if __name__ == "__main__":
    file_handler: FolderStructure = FolderStructureLocal("./config/config.yaml")
    WorkflowImpl(file_handler).run_flow()
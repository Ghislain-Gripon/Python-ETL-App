from src.FolderStructure import FolderStructure
from src.FolderStructureLocal import FolderStructureLocal
from src.WorkflowImp import WorkflowImp

if __name__ == "__main__":
    file_handler: FolderStructure = FolderStructureLocal("./config/config.yaml")
    WorkflowImp(file_handler)
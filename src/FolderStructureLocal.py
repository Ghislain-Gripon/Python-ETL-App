from src.FolderStructure import FolderStructure


class FolderStructureLocal(FolderStructure):

    def __init__(self, config_file_path = None, config = None):
        FolderStructure.__init__(config_file_path, config)

    def move(self, source: str, target: str):
        pass

    def load(self, file_path: str):
        pass

    def read_yaml(self, file_stream) -> dict:
        pass

    def get_config(self, ) -> dict:
        """
        Returns the configuration dictionary.
        """
        pass
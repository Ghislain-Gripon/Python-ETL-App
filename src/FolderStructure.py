class FolderStructure:

    def __init__(self, config_file_path = None):
        self.config_file_path = config_file_path
        self.config:dict = dict()

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
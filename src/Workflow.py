from src.DataLoader import DataLoader
from src.DataLoaderCSV import DataLoaderCSV
from src.DataLoaderJSON import DataLoaderJSON
from src.Decorators import debug
from src.FolderStructure import FolderStructure


class Workflow:

    @debug
    def __init__(self, _file_handler: FolderStructure):
        self.file_handler: FolderStructure = _file_handler
        self.config: dict = self.file_handler.get_config()
        self.data_loaders: dict = dict()

    @debug
    def run_flow(self, ):
        pass

    @debug
    def get_data_loader_class(self, file_extension: str) -> DataLoader:
        """
        Fetches the DataLoader class corresponding to file_extension

        :return: Instance of DataLoader class
        """
        if file_extension in self.data_loaders:
            return self.data_loaders[file_extension]

        match file_extension:
            case "csv":
                self.data_loaders["csv"] = DataLoaderCSV()
            case "json":
                self.data_loaders["json"] = DataLoaderJSON()
            case _:
                raise ValueError(f"'{file_extension}' extension is not supported or does not exist")

        return self.data_loaders[file_extension]

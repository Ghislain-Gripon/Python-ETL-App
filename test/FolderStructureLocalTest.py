import unittest
from os.path import exists
from pathlib import Path

from src.file_system.FolderStructureLocal import FolderStructureLocal

class FolderStructureLocalTest(unittest.TestCase):

    def test_move_file_target_not_exists(self):
        def test_file_moved():
            self.assertTrue(exists(Path("data/target_test/file.txt")))

        file_handler:FolderStructureLocal = FolderStructureLocal(Path("config/config.yaml"))
        file_handler.move(Path("data/source_test/file.txt"), Path("data/target_test/file.txt"))

        test_file_moved()

        self.assertEqual(file_handler.move(Path("data/target_test/file.txt"), Path("data/source_test/file.txt")), Path("data/source_test/file.txt"))

    def test_move_file_target_already_exists(self):
        file_handler: FolderStructureLocal = FolderStructureLocal(Path("config/config.yaml"))
        self.assertRaises(FileExistsError, file_handler.move, Path("data/source_test/file_exists.txt"), Path("data/source_test/file_exists.txt"))

    def test_move_file_source_not_exists(self):
        file_handler: FolderStructureLocal = FolderStructureLocal(Path("config/config.yaml"))
        self.assertRaises(FileNotFoundError, file_handler.move, Path("data/source_test/nofile.txt"), Path("data.target_test/nofile.txt"))

    def test_load_no_file(self):
        file_handler:FolderStructureLocal = FolderStructureLocal(Path("config/config.yaml"))
        self.assertRaises(FileNotFoundError, file_handler.load, Path("data/source_test/no_such_file.txt"))

    def test_load_file_get_stream_by_Path(self):
        file_handler:FolderStructureLocal = FolderStructureLocal(Path("config/config.yaml"))
        ref_text:str = "some text"
        test_stream = file_handler.load(Path("data/source_test/file.txt"))
        test_text:str = test_stream.read()
        test_stream.close()

        self.assertEqual(ref_text, test_text)

    def test_load_file_get_stream_by_string(self):
        file_handler:FolderStructureLocal = FolderStructureLocal(Path("config/config.yaml"))
        ref_text:str = "some text"
        test_stream = file_handler.load("data/source_test/file.txt")
        test_text:str = test_stream.read()
        test_stream.close()

        self.assertEqual(ref_text, test_text)

    def test_read_yaml_as_stream(self):
        ref_dict:dict = dict({
            "execution_environment": {
                "local": {
                    "data_origin": 'csv',
                    "data_directory_path": {
                        "base_path": "data",
                        "data": {
                            "input": {
                                "base_path": "input",
                                "directories": [
                                    "inbound",
                                    "work"
                                ]
                            },
                            "output": {
                                "base_path": "output",
                                "directories": [
                                    "error",
                                    "done"
                                ]
                            }
                        },
                        "config": {
                            "directories": {
                                "config": "config",
                                "flows": "flows"
                            },
                            "files": {
                                "logger_config_path": "logger_config.yaml",
                                "flows_path": "flows.yaml"
                            }
                        }
                    }
                }
            }
        })
        file_handler:FolderStructureLocal = FolderStructureLocal(Path("config/config.yaml"))

        test_dict_stream = file_handler.load(Path("data/test.yaml"))
        test_dict:dict = file_handler.read_yaml(test_dict_stream)
        test_dict_stream.close()

        self.assertDictEqual(ref_dict, test_dict)

    def test_read_yaml_from_file_path(self):
        ref_dict:dict = dict({
            "execution_environment": {
                "local": {
                    "data_origin": 'csv',
                    "data_directory_path": {
                        "base_path": "data",
                        "data": {
                            "input": {
                                "base_path": "input",
                                "directories": [
                                    "inbound",
                                    "work"
                                ]
                            },
                            "output": {
                                "base_path": "output",
                                "directories": [
                                    "error",
                                    "done"
                                ]
                            }
                        },
                        "config": {
                            "directories": {
                                "config": "config",
                                "flows": "flows"
                            },
                            "files": {
                                "logger_config_path": "logger_config.yaml",
                                "flows_path": "flows.yaml"
                            }
                        }
                    }
                }
            }
        })
        file_handler:FolderStructureLocal = FolderStructureLocal(Path("config/config.yaml"))

        test_dict_stream = file_handler.load(Path("data/test.yaml"))
        test_dict: dict = file_handler.read_yaml(test_dict_stream)
        test_dict_stream.close()

        self.assertDictEqual(ref_dict, test_dict)

if __name__ == '__main__':
    unittest.main()

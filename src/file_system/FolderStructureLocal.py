import logging
import logging.config
import re

from typing import TextIO

from file_system.FolderStructure import FolderStructure
from pathlib import Path
from Decorators import debug
from yaml import safe_load


class FolderStructureLocal(FolderStructure):

	def __init__(self, _config_file_path: Path):

		config_file_path: Path = Path(_config_file_path) if _config_file_path is not None else Path(
			"data/config/config.yaml"
		)
		super().__init__(config_file_path)

		config_file_stream = self.load(Path(self.config_file_path))
		self.config: dict = self.read_yaml(config_file_stream)["execution_environment"]["local"]
		config_file_stream.close()

		logger_config_path: Path = Path(self.config["data_directory_path"]["config"]["directories"]["config"])/ \
								   self.config["data_directory_path"]["config"]["files"]["logger_config"]

		with self.load(logger_config_path) as f:
			logging.config.dictConfig(self.read_yaml(f))

		for io_direction in ["input", "output"]:

			for directory_name in self.config["data_directory_path"]["data"][io_direction]["directories"]:
				full_directory: Path = (Path(self.config["data_directory_path"]["base_path"])
										/self.config["data_directory_path"]["data"][io_direction]["base_path"]
										/directory_name
										)
				full_directory.mkdir(parents=True, exist_ok=True)
				self.file_directories[directory_name] = full_directory

	@debug
	def move(self, source: str | Path, target: str | Path) -> Path:

		source, target = Path(source), Path(target)

		if target.is_file():
			raise FileExistsError(f"{target} already exists")

		return source.rename(target)

	@debug
	def load(self, file_path: str | Path) -> TextIO:
		path: Path = Path(file_path)
		if Path.is_file(path):
			encoding = "utf-8-sig" if self.config.get("encoding") is None else self.config["encoding"]
			return open(path, 'r', encoding=encoding)
		else:
			logging.error(f"No file located at {file_path}")
			raise FileNotFoundError(f"There is no file at {path}")

	@debug
	def read_yaml(self, file_stream) -> dict:
		"""
				Safe loads a yaml dictionary from an open file stream.
				"""

		_file: dict[str, str | dict] = dict()

		try:
			_file = safe_load(file_stream)

		# catch a yaml related error to inform user of problem with config file
		except FileNotFoundError as e:
			logging.error("YAML file at {} couldn't be decoded.".format(file_stream))
			raise e

		except PermissionError as e:
			logging.error("Can not access {}, permission denied.".format(file_stream))
			raise e

		except BaseException as e:
			logging.error(f"{type(e)} : {e.args}")
			raise e

		return _file

	@debug
	def get_file_list(self, regex: str) -> list[Path]:
		return [file for file in self.file_directories["inbound"].iterdir() if re.search(regex, file.name) is not None]

	@debug
	def get_config(self, ) -> dict:
		return self.config

	@debug
	def exists(self, file_path: Path | str) -> bool:
		return Path(file_path).exists()

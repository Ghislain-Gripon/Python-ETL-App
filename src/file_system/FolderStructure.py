from pathlib import Path
from abc import abstractmethod


class FolderStructure:

	def __init__(self, config_file_path: Path):
		self.config_file_path: Path = Path(config_file_path)

		self.config: dict = dict()

		self.file_directories: dict[str, Path] = dict()

	@classmethod
	@abstractmethod
	def move(cls, source: Path, target: Path) -> Path:
		pass

	@classmethod
	@abstractmethod
	def load(cls, file_path: Path):
		pass

	@classmethod
	@abstractmethod
	def read_yaml(cls, file_stream) -> dict[str, dict | str]:
		pass

	@classmethod
	@abstractmethod
	def get_file_list(cls, regex: str) -> list[Path]:
		pass

	@classmethod
	@abstractmethod
	def get_config(cls, ) -> dict[str, dict | str]:
		"""
		Returns the configuration dictionary.
		"""
		pass

	@classmethod
	@abstractmethod
	def exists(cls, file_path: Path) -> bool:
		pass

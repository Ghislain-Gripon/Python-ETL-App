from pathlib import Path


class FolderStructure:

	def __init__(self, config_file_path=None):
		self.config_file_path = config_file_path
		self.config: dict = dict()
		self.file_directories = dict()

	def move(self, source: str | Path, target: str | Path) -> Path:
		pass

	def load(self, file_path: str | Path):
		pass

	def read_yaml(self, file_stream) -> dict:
		pass

	def get_file_list(self, regex: str) -> list[Path]:
		pass

	def get_config(self, ) -> dict:
		"""
		Returns the configuration dictionary.
		"""
		pass

	def exists(self, file_path: Path | str) -> bool:
		pass

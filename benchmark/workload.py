import abc
import os

from path import Path


class Workload(abc.ABC):

    DATASETS_FOLDER = Path('datasets/')
    QUERIES_FOLDER = Path('queries/')

    def __init__(self):
        pass

    @abc.abstractmethod
    def get_name(self):
        pass

    @abc.abstractmethod
    def get_table_name(self):
        pass

    def get_dataset_folder(self):
        return os.path.abspath(os.path.join(self.DATASETS_FOLDER, self.get_name()))

    def get_files_pattern(self):
        return f'{self.get_dataset_folder()}/{self.get_table_name()}*.parquet'

    def get_queries_folder(self):
        return os.path.abspath(os.path.join(self.QUERIES_FOLDER, self.get_name()))

    @abc.abstractmethod
    def generate_dataset(self, **params):
        pass

    @abc.abstractmethod
    def generate_queries(self):
        pass

    @abc.abstractmethod
    def is_dataset_generated(self) -> bool:
        pass

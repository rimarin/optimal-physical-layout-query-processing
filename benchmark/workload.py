import abc
import duckdb
import os

from path import Path


class Workload(abc.ABC):

    DATASETS_FOLDER = Path('datasets/')
    QUERIES_FOLDER = Path('queries/')

    def __init__(self):
        self.total_rows = None

    @abc.abstractmethod
    def get_name(self):
        pass

    @abc.abstractmethod
    def get_table_name(self):
        pass

    def get_total_rows(self):
        if not self.total_rows:
            self.total_rows = len(duckdb.sql(f'SELECT * FROM read_parquet(\'{self.get_files_pattern()}\')'))
        return self.total_rows

    def get_dataset_folder(self):
        return os.path.abspath(os.path.join(self.DATASETS_FOLDER, self.get_name()))

    def get_files_pattern(self):
        return f'{self.get_dataset_folder()}/{self.get_table_name()}*.parquet'

    def get_queries_folder(self):
        return os.path.abspath(os.path.join(self.QUERIES_FOLDER, self.get_name()))

    def get_generated_queries_folder(self):
        return os.path.abspath(os.path.join(self.get_queries_folder(), 'generated'))

    @abc.abstractmethod
    def generate_dataset(self, **params):
        pass

    @abc.abstractmethod
    def generate_queries(self):
        pass

    @abc.abstractmethod
    def is_dataset_generated(self) -> bool:
        pass

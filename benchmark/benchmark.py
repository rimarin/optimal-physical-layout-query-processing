import abc
import duckdb
import os

from settings import DATA_FORMAT, NO_PARTITION


class Benchmark(abc.ABC):

    DATASETS_FOLDER = os.path.abspath('datasets/')
    QUERIES_FOLDER = os.path.abspath('queries/')

    def __init__(self):
        self.total_rows = None

    @abc.abstractmethod
    def generate_dataset(self, **params):
        pass

    @abc.abstractmethod
    def generate_queries(self):
        pass

    def get_dataset_folder(self, partitioning=NO_PARTITION):
        dataset_folder = os.path.abspath(os.path.join(self.DATASETS_FOLDER, self.get_name(), partitioning))
        if not os.path.exists(dataset_folder):
            os.makedirs(dataset_folder)
        return dataset_folder

    def get_num_total_partitions(self, partitioning):
        return len([f for f in os.listdir(self.get_dataset_folder(partitioning)) if f.endswith(DATA_FORMAT)])

    def get_files_pattern(self):
        return f'{self.get_dataset_folder()}/{self.get_name()}*{DATA_FORMAT}'

    def get_generated_queries_folder(self):
        return os.path.abspath(os.path.join(self.get_queries_folder(), 'generated'))

    def get_num_queries(self):
        return len([f.endswith(".sql") for f in os.listdir(self.get_generated_queries_folder())])

    @staticmethod
    def get_partitioning_columns():
        raise NotImplementedError

    @staticmethod
    def get_query_columns(query_number):
        raise NotImplementedError

    @staticmethod
    def get_query_selectivity(query):
        raise NotImplementedError

    @abc.abstractmethod
    def get_name(self):
        pass

    def get_queries_folder(self):
        return os.path.abspath(os.path.join(self.QUERIES_FOLDER, self.get_name()))

    def get_total_rows(self):
        if not self.total_rows:
            self.total_rows = len(duckdb.sql(f'SELECT * FROM read_parquet(\'{self.get_files_pattern()}\')'))
        return self.total_rows

    def is_dataset_generated(self) -> bool:
        return os.path.exists(os.path.join(self.get_dataset_folder(), self.get_name() + '.parquet'))

    def is_query_workload_generated(self) -> bool:
        return any(f.endswith(".sql") for f in os.listdir(self.get_generated_queries_folder()))

    @staticmethod
    def rename_queries(path: str):

        def num_to_letter(num: int) -> str:
            return chr(ord('a') + num)

        files = sorted(os.listdir(path))
        grouped_files = [[] for _ in range(7)]
        for file in files:
            if '_' in file:
                idx = int(file.split('_')[0]) - 1
                grouped_files[idx].append(file)
        for group in grouped_files:
            for index, file in enumerate(group):
                renamed_file = file.split('_')[0] + num_to_letter(index) + '.sql'
                os.rename(os.path.join(path, file), os.path.join(path, renamed_file))

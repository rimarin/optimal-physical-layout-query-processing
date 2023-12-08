import statistics

from query_parser import QueryParser
from storage_manager import StorageManager


class BenchmarkResult:
    def __init__(self, benchmark, benchmark_config, latencies, used_partitions):
        self.benchmark = benchmark
        self.dataset = benchmark_config.dataset
        self.partitioning = benchmark_config.partitioning
        self.partition_size = benchmark_config.partition_size
        self.query_number = benchmark_config.query_number
        self.query_variant = benchmark_config.query_variant
        self.partitioning_columns = benchmark_config.partitioning_columns
        self.used_columns = benchmark_config.partitioning_columns
        self.latency_avg = statistics.mean(latencies)
        self.latency_std = statistics.stdev(latencies)
        self.used_partitions = used_partitions
        self.total_partitions = benchmark_config.total_partitions
        self.storage_manager = StorageManager(self.benchmark.get_dataset_folder(self.partitioning))
        with open('temp/query.sql') as query_file:
            self.query_parser = QueryParser(query_file.read())


    def __str__(self):
        return f'{self.dataset} {self.partitioning} q{self.query_number}{self.query_variant}'

    def format(self, filetype='csv'):
        self.used_columns = QueryParser()
        return (f'{self.dataset}; {self.partitioning}; q{self.query_number}{self.query_variant}; '
                f'{self.partitioning_columns}; {self.query_parser.extract_columns_from_where()}; '
                f'{self.latency_avg}; {self.latency_std}; {self.used_partitions}; {self.total_partitions}; \n')
        # TODO: add partition size

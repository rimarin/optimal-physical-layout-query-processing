import datetime
import statistics

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
        self.latencies = latencies
        self.latency_avg = statistics.mean(latencies)
        self.latency_std = statistics.stdev(latencies)
        self.used_partitions = used_partitions
        self.total_partitions = benchmark_config.total_partitions

    def __str__(self):
        return f'{self.dataset} {self.partitioning} q{self.query_number}{self.query_variant}'

    def format(self, filetype='csv'):
        self.total_partitions = StorageManager.get_num_files(self.benchmark.get_dataset_folder(self.partitioning))
        self.used_columns = self.benchmark.get_query_columns(self.query_number)
        return (f'{self.dataset};{self.partitioning};q{self.query_number}{self.query_variant};'
                f'{self.benchmark.get_query_selectivity(str(self.query_number) + self.query_variant)};'
                f'{self.partitioning_columns};{len(self.partitioning_columns)};{self.used_columns};'
                f'{self.latencies};{self.latency_avg};{self.latency_std};{self.partition_size};'
                f'{self.used_partitions};{self.total_partitions};{datetime.datetime.now()}\n')

    @staticmethod
    def format_header():
        return ('dataset;partitioning;query;selectivity;partitioning_columns;num_partitioning_columns;'
                'used_columns;latencies;latency_avg;latency_std;partition_size;used_partitions;total_partitions;'
                'timestamp\n')

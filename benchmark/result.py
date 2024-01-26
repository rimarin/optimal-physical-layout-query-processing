import datetime
import statistics

from storage_manager import StorageManager


class BenchmarkResult:
    def __init__(self, benchmark_instance, latencies, used_partitions, average_partition_size):
        self.instance = benchmark_instance
        self.benchmark = benchmark_instance.benchmark
        self.config = benchmark_instance.config
        self.used_columns = self.config.partitioning_columns
        self.total_partitions = self.config.total_partitions
        self.num_rows = self.benchmark.get_total_rows()
        self.latencies = latencies
        if len(latencies) > 0:
            self.latency_avg = statistics.mean(latencies) if len(latencies) > 1 else latencies[0]
            self.latency_std = statistics.stdev(latencies) if len(latencies) > 1 else latencies[0]
        else:
            self.latency_avg = 0
            self.latency_std = 0
        self.used_partitions = used_partitions
        self.average_partition_size = average_partition_size
        self.query_str = f'{str(self.instance.query_number)}{self.instance.query_variant}'

    def __str__(self):
        return f'{self.config.dataset} {self.config.partitioning} {self.query_str}'

    def format(self, filetype='csv'):
        try:
            self.total_partitions = StorageManager.get_num_files(self.benchmark.get_dataset_folder(self.config.partitioning))
        except Exception as e:
            self.total_partitions = 0
        try:
            self.used_columns = self.benchmark.get_query_columns(self.instance.query_number)
        except Exception as e:
            self.used_columns = []
        return (f'{self.config.dataset};{self.num_rows};{self.config.partitioning};{self.config.time_to_partition};'
                f'q{self.query_str};{self.benchmark.get_query_selectivity(self.query_str)};'
                f'{self.config.partitioning_columns};{len(self.config.partitioning_columns)};{self.used_columns};'
                f'{len(self.used_columns)};{self.latencies};{self.latency_avg};{self.latency_std};'
                f'{self.config.partition_size};{self.average_partition_size};{self.used_partitions};'
                f'{self.total_partitions};{datetime.datetime.now()}\n')

    @staticmethod
    def format_header():
        return ('dataset;num_rows;partitioning;time_to_partition;query;selectivity;partitioning_columns;'
                'num_partitioning_columns;used_columns;num_used_columns;latencies;latency_avg;latency_std;'
                'partition_size;partition_size_mb;used_partitions;total_partitions;timestamp\n')

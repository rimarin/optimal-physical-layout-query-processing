import statistics


class BenchmarkResult:
    def __init__(self, benchmark_config, latencies, read_partitions):
        self.dataset = benchmark_config.dataset
        self.partitioning = benchmark_config.partitioning
        self.partition_size = benchmark_config.partition_size
        self.query_number = benchmark_config.query_number
        self.query_variant = benchmark_config.query_variant
        self.columns = benchmark_config.columns
        self.latency_avg = statistics.mean(latencies)
        self.latency_std = statistics.stdev(latencies)
        self.read_partitions = read_partitions

    def __str__(self):
        return f'{self.dataset} {self.partitioning} q{self.query_number}{self.query_number}'

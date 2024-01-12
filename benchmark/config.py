class BenchmarkConfig:
    def __init__(self, dataset: str, partitioning: str, partition_size: int, partitioning_columns: list, results_file: str):
        self.dataset = dataset
        self.partitioning = partitioning
        self.partition_size = partition_size
        self.partitioning_columns = partitioning_columns
        self.total_partitions = 0
        self.results_file = results_file

    def __str__(self):
        return f'{self.dataset} {self.partitioning} {self.partition_size} {self.partitioning_columns}'

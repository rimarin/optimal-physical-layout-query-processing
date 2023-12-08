class BenchmarkConfig:
    def __init__(self, dataset: str, partitioning: str, partition_size: int, query_number: int, query_variant: str,
                 partitioning_columns: list, results_file: str, total_partitions: int = 0):
        self.dataset = dataset
        self.partitioning = partitioning
        self.partition_size = partition_size
        self.query_number = query_number
        self.query_variant = query_variant
        self.partitioning_columns = partitioning_columns
        self.total_partitions = total_partitions
        self.results_file = results_file
        # TODO: add parameter for selected partitioning_columns (partitioning_columns used in the query predicate)

    def __str__(self):
        return f'{self.dataset} {self.partitioning} {self.partition_size} q{self.query_number}{self.query_variant}'

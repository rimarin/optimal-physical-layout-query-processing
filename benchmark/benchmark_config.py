class BenchmarkConfig:
    def __init__(self, dataset: str, partitioning: str, partition_size: int, query_number: int, query_variant: str,
                 columns: list):
        self.dataset = dataset
        self.partitioning = partitioning
        self.partition_size = partition_size
        self.query_number = query_number
        self.query_variant = query_variant
        self.columns = columns

    def __str__(self):
        return f'{self.dataset} {self.partitioning} q{self.query_number}{self.query_number}'

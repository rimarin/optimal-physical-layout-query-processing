import os

from itertools import combinations

from benchmark_config import BenchmarkConfig
from benchmark_instance import BenchmarkInstance


DATASETS = ['tpch-sf1', 'osm', 'taxi']
PARTITIONINGS = ['hilbert-curve', 'kd-tree', 'quad-tree', 'str-tree', 'z-curve-order'] # 'fixed-grid', 'grid-file'
PARTITION_SIZES = [1000, 10000, 50000, 100000, 250000, 500000]

RESULTS_FOLDER = 'results'
RESULTS_FILE = os.path.join(RESULTS_FOLDER, 'results.csv')


def run_benchmarks(datasets: list, partitionings: list, partition_sizes: list):
    if not os.path.exists(RESULTS_FILE):
        with open(RESULTS_FILE, 'w') as result_file:
            result_file.write('dataset; partitioning; query; partitioning_columns; used_columns; latency_avg; '
                              'latency_std; used_partitions; total_partitions; \n')
    for dataset in datasets:
        for partitioning in partitionings:
            for partition_size in partition_sizes:
                benchmark = BenchmarkInstance.get_benchmark(dataset)
                query_files = [f for f in os.listdir(benchmark.get_generated_queries_folder())
                               if f.endswith(".sql")]
                min_num_dimensions = 2
                columns = benchmark.get_relevant_columns()
                columns_combinations = sum([list(map(list, combinations(columns, i)))
                                            for i in range(min_num_dimensions, len(columns) + 1)], [])
                for columns_combination in columns_combinations:
                    for i, query_file in enumerate(query_files):
                        query_file_name = query_file.split('.sql')[0]
                        num_query = int(''.join(filter(str.isdigit, query_file_name)))
                        query_variant = ''.join(filter(str.isalpha, query_file_name))
                        benchmark_instance = BenchmarkInstance(BenchmarkConfig(
                            dataset=dataset,
                            partitioning=partitioning,
                            partition_size=partition_size,
                            query_number=num_query,
                            query_variant=query_variant,
                            partitioning_columns=columns_combination,
                            results_file=RESULTS_FILE
                        ))
                        benchmark_instance.prepare_dataset()
                        benchmark_instance.prepare_queries()
                        if i == 0:
                            benchmark_instance.generate_partitions()
                        benchmark_instance.run()
                    benchmark_instance.cleanup()


if __name__ == "__main__":
    run_benchmarks(DATASETS, PARTITIONINGS, PARTITION_SIZES)

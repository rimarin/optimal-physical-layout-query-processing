import os
import re
import subprocess

from itertools import combinations

from benchmark_config import BenchmarkConfig
from benchmark_result import BenchmarkResult
from workload_osm import OSMWorkload
from workload_taxi import TaxiWorkload
from workload_tpch import TPCHWorkload

DATASETS = ['tpch-sf1', 'osm', 'taxi']
PARTITIONING = ['hilbert-curve', 'kd-tree', 'quad-tree', 'str-tree', 'z-curve-order'] # 'fixed-grid', 'grid-file'
PARTITION_SIZES = [1000, 10000, 50000, 100000, 250000, 500000]


class Benchmark:
    def __init__(self, config: BenchmarkConfig = None):
        self.result = None
        self.duckdb_path = None
        self.config = config
        self.dataset_workload = self.get_workload(self.config.dataset)

    def generate_partitions(self):
        pass
        os.system(
            f'../cmake-build-release/partitioner {self.config.dataset} {self.config.partitioning} {self.config.partition_size} '
            f'{",".join(self.config.columns)}')

    @staticmethod
    def get_workload(name):
        scale = 1
        if 'tpch-sf' in name:
            scale = name.split('tpch-sf')[1]
        name_to_workload = {
            f'tpch-sf{scale}': TPCHWorkload(scale=scale),
            'taxi': TaxiWorkload(),
            'osm': OSMWorkload()
        }
        if name in name_to_workload:
            return name_to_workload[name]
        raise Exception('Workload not found')

    def prepare_dataset(self):
        if not self.dataset_workload.is_dataset_generated():
            self.dataset_workload.generate_dataset()

    def prepare_queries(self):
        if not self.dataset_workload.is_query_workload_generated():
            self.dataset_workload.generate_queries()

    def runner_prepare(self):
        """
        - Download DuckDB benchmarking suite (with our custom benchmark)
        - Copy generated query to a temporary folder
        - Adapt the query to the dataset by changing the read_parquet(...) expression content
        - Write out the updated query
        """
        duckdb_folder = "duckdb"
        self.duckdb_path = os.path.abspath(os.path.join(os.getcwd(), "../../", duckdb_folder))
        if not os.path.exists(self.duckdb_path):
            os.system("git clone https://github.com/rimarin/duckdb")
            os.system(f"cd {self.duckdb_path}")
        runner_path = os.path.join(self.duckdb_path, 'build/release/benchmark/benchmark_runner')
        if not os.path.exists(runner_path):
            os.system("BUILD_BENCHMARK=1 make")

        tmp_folder = os.path.join(os.path.abspath(os.getcwd()), 'temp')
        generated_query_file = f"{self.dataset_workload.get_generated_queries_folder()}/{self.config.query_number}{self.config.query_variant}.sql"
        os.system(f"cp {generated_query_file} {tmp_folder}")
        moved_query_file = os.path.join(tmp_folder, f'{self.config.query_number}{self.config.query_variant}.sql')
        tmp_query_file = os.path.join(tmp_folder, "query.sql")
        os.system(f"mv {moved_query_file} {tmp_query_file}")

        with open(tmp_query_file, 'r') as query_file:
            original_query = query_file.read()
        from_clause = f'FROM read_parquet(\'{self.dataset_workload.get_dataset_folder(self.config.partitioning)}/*.parquet\')'
        query = re.sub('FROM?(.*?)where', f'{from_clause} where', original_query, flags=re.DOTALL)
        with open(tmp_query_file, 'w') as query_file:
            query_file.write(query)

    def runner_launch(self):
        result_rows = str(subprocess.check_output(
            [f'{self.duckdb_path}/build/release/benchmark/benchmark_runner PartitioningBenchmark'],
            shell=True, text=True, stderr=subprocess.STDOUT)).split('\n')
        latencies = []
        for row in result_rows[1:]:
            try:
                str_value = row.split('\t')[-1]
                latency = float(str_value)
                latencies.append(latency)
            except Exception as e:
                pass
        # TODO: get num partitions from log file
        self.result = BenchmarkResult(self.config, latencies, 0)
        pass

    def collect_results(self):
        pass

    def run(self):
        self.prepare_dataset()
        self.prepare_queries()
        self.generate_partitions()
        self.runner_prepare()
        self.runner_launch()
        self.collect_results()
        self.cleanup()

    def cleanup(self):
        # TODO: support retrieval from AWS S3 at some point
        os.system(f'rm datasets/{self.config.dataset}/{self.config.partitioning}/*.parquet')


def run_benchmarks(datasets: list, partitioning: list, partition_sizes: list):
    for dataset in datasets:
        for partitioning_type in partitioning:
            for partition_size in partition_sizes:
                dataset_workload = Benchmark.get_workload(dataset)
                query_files = [f for f in os.listdir(dataset_workload.get_generated_queries_folder())
                               if f.endswith(".sql")]
                for query_file in query_files:
                    query_file_name = query_file.split('.sql')[0]
                    num_query = int(''.join(filter(str.isdigit, query_file_name)))
                    query_variant = ''.join(filter(str.isalpha, query_file_name))
                    total_partitions = dataset_workload.get_num_total_partitions()
                    columns = dataset_workload.get_relevant_columns()
                    min_num_dimensions = 2
                    columns_combinations = sum([list(map(list, combinations(columns, i)))
                                                for i in range(min_num_dimensions, len(columns) + 1)], [])
                    for columns_combination in columns_combinations:
                        benchmark = Benchmark(BenchmarkConfig(
                            dataset=dataset,
                            partitioning=partitioning_type,
                            partition_size=partition_size,
                            query_number=num_query,
                            query_variant=query_variant,
                            columns=columns_combination,
                            total_partitions=total_partitions
                        ))
                        benchmark.run()


if __name__ == "__main__":
    run_benchmarks(DATASETS, PARTITIONING, PARTITION_SIZES)

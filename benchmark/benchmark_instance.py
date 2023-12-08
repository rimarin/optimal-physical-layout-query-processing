import os
import re
import subprocess

from benchmark_config import BenchmarkConfig
from benchmark_result import BenchmarkResult
from benchmarks.benchmark_osm import BenchmarkOSM
from benchmarks.benchmark_taxi import BenchmarkTaxi
from benchmarks.benchmark_tpch import BenchmarkTPCH


class BenchmarkInstance:
    def __init__(self, config: BenchmarkConfig = None):
        self.result = None
        self.duckdb_path = None
        self.config = config
        self.benchmark = self.get_benchmark(self.config.dataset)

    def generate_partitions(self):
        subprocess.run([f'../cmake-build-release/partitioner/partitioner', f'{self.config.dataset}',
                        f'{self.config.partitioning}', f'{self.config.partition_size}',
                        f'{",".join(self.config.partitioning_columns)}'], stdout=subprocess.DEVNULL)
        self.config.total_partitions = self.benchmark.get_num_total_partitions(self.config.partitioning)
        print(f'Partitioned dataset into {self.config.total_partitions} partitions')

    @staticmethod
    def get_benchmark(name):
        scale = 1
        if 'tpch-sf' in name:
            scale = name.split('tpch-sf')[1]
        name_to_workload = {
            f'tpch-sf{scale}': BenchmarkTPCH(scale=scale),
            'taxi': BenchmarkTaxi(),
            'osm': BenchmarkOSM()
        }
        if name in name_to_workload:
            return name_to_workload[name]
        raise Exception('Benchmark not found')

    def prepare_dataset(self):
        if not self.benchmark.is_dataset_generated():
            self.benchmark.generate_dataset()

    def prepare_queries(self):
        if not self.benchmark.is_query_workload_generated():
            self.benchmark.generate_queries()

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
            subprocess.check_output("git clone https://github.com/rimarin/duckdb", shell=True)
            subprocess.check_output(f"cd {self.duckdb_path}", shell=True)
        runner_path = os.path.join(self.duckdb_path, 'build/release/benchmark/benchmark_runner')
        if not os.path.exists(runner_path):
            subprocess.check_output("BUILD_BENCHMARK=1 make", shell=True)

        tmp_folder = os.path.join(os.path.abspath(os.getcwd()), 'temp')
        generated_query_file = f"{self.benchmark.get_generated_queries_folder()}/{self.config.query_number}{self.config.query_variant}.sql"
        subprocess.check_output(f"cp {generated_query_file} {tmp_folder}", shell=True)
        moved_query_file = os.path.join(tmp_folder, f'{self.config.query_number}{self.config.query_variant}.sql')
        tmp_query_file = os.path.join(tmp_folder, "query.sql")
        subprocess.check_output(f"mv {moved_query_file} {tmp_query_file}", shell=True)

        with open(tmp_query_file, 'r') as query_file:
            original_query = query_file.read()
        from_clause = f'FROM read_parquet(\'{self.benchmark.get_dataset_folder(self.config.partitioning)}/*.parquet\')'
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
        self.result = BenchmarkResult(self.benchmark, self.config, latencies, 0)

    def collect_results(self):
        with open(self.config.results_file, 'a') as result_file:
            result_file.write(self.result.format())
        print(f'Completed run for benchmark {self.config}')

    def run(self):
        self.runner_prepare()
        self.runner_launch()
        self.collect_results()

    def cleanup(self):
        # TODO: support retrieval from AWS S3 at some point
        subprocess.run(['rm', f'{self.benchmark.get_dataset_folder(self.config.partitioning)}/*.parquet'],
                       shell=True, stdout=subprocess.DEVNULL)
        print(f'Removed parquet files from folder {self.benchmark.get_dataset_folder(self.config.partitioning)}')

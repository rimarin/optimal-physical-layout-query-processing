import logging
import os
import re
import subprocess

from config import BenchmarkConfig
from result import BenchmarkResult
from benchmarks.osm import BenchmarkOSM
from benchmarks.taxi import BenchmarkTaxi
from benchmarks.tpch import BenchmarkTPCH
from settings import DATA_FORMAT, PARTITIONS_LOG_FILE, NO_PARTITION
from storage_manager import StorageManager


class BenchmarkInstance:
    def __init__(self, config: BenchmarkConfig, logger: logging.Logger):
        self.result = None
        self.duckdb_path = None
        self.config = config
        self.benchmark = self.get_benchmark(self.config.dataset)
        self.query_number = None
        self.query_variant = None
        self.logger = logger

    def set_query(self, query_number, query_variant):
        self.query_number = query_number
        self.query_variant = query_variant

    def generate_partitions(self):
        partitioner_command = [f'../cmake-build-release/partitioner/partitioner',
                               f'{self.benchmark.get_dataset_folder("")}',
                               f'{self.config.dataset}',
                               f'{self.config.partitioning}',
                               f'{self.config.partition_size}',
                               f'{",".join(self.config.partitioning_columns)}']
        try:
            subprocess.run(partitioner_command)
        except subprocess.SubprocessError as e:
            self.logger.warning(f'Partitioning failed: {str(e)}')

        self.config.total_partitions = self.benchmark.get_num_total_partitions(self.config.partitioning)
        if self.config.total_partitions == 0:
            self.logger.warning('No partitions, something could be wrong. Used command:')
            self.logger.warning(f'{" ".join(partitioner_command)}')

    @staticmethod
    def get_benchmark(name):
        """
        Retrieve the Benchmark object according to the provided name
        """
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

    @staticmethod
    def get_query_files(benchmark):
        """
        List of sql queries files available in the generated query folder of the used dataset
        """
        return [f for f in os.listdir(benchmark.get_generated_queries_folder()) if f.endswith(".sql")]

    @staticmethod
    def get_query(query_file_name):
        query_file_name = query_file_name.split('.sql')[0]
        query_number = int(''.join(filter(str.isdigit, query_file_name)))
        query_variant = ''.join(filter(str.isalpha, query_file_name))
        return query_number, query_variant

    def prepare_dataset(self):
        """
        Generate the dataset if it does not exist already
        """
        if not self.benchmark.is_dataset_generated():
            self.logger.info("Generating dataset")
            self.benchmark.generate_dataset()

    def prepare_queries(self):
        """
        Generate the queries if they do not exist already
        """
        if not self.benchmark.is_query_workload_generated():
            self.logger.info("Generating queries")
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
            self.logger.info("Downloading duckdb")
            subprocess.check_output("git clone https://github.com/rimarin/duckdb", shell=True)
            subprocess.check_output(f"cd {self.duckdb_path}", shell=True)
        runner_path = os.path.join(self.duckdb_path, 'build/release/benchmark/benchmark_runner')
        if not os.path.exists(runner_path):
            self.logger.info("Building benchmark runner")
            subprocess.check_output("BUILD_BENCHMARK=1 make", shell=True)

        tmp_folder = os.path.join(os.path.abspath(os.getcwd()), 'temp')
        if not os.path.exists(tmp_folder):
            os.makedirs(tmp_folder)
        generated_query_file = f"{self.benchmark.get_generated_queries_folder()}/{self.query_number}{self.config.query_variant}.sql"
        subprocess.check_output(f"cp {generated_query_file} {tmp_folder}", shell=True)
        moved_query_file = os.path.join(tmp_folder, f'{self.query_number}{self.query_variant}.sql')
        tmp_query_path = os.path.join(self.duckdb_path, "query.sql")
        subprocess.check_output(f"mv {moved_query_file} {tmp_query_path}", shell=True)

        with open(tmp_query_path, 'r') as tmp_query_file:
            original_query = tmp_query_file.read()
        from_clause = f'FROM read_parquet(\'{self.benchmark.get_dataset_folder(self.config.partitioning)}/*{DATA_FORMAT}\')'
        tmp_query = re.sub('FROM?(.*?)where', f'{from_clause} where', original_query, flags=re.DOTALL)
        with open(tmp_query_path, 'w') as tmp_query_file:
            tmp_query_file.write(tmp_query)

        verify_query_path = os.path.join(self.duckdb_path, "verify.sql")
        from_clause = f'FROM read_parquet(\'{self.benchmark.get_dataset_folder(NO_PARTITION)}/*{DATA_FORMAT}\')'
        verify_query = re.sub('FROM?(.*?)where', f'{from_clause} where', original_query, flags=re.DOTALL)
        with open(verify_query_path, 'w') as verify_query_file:
            verify_query_file.write(verify_query)

    def runner_launch(self):
        retries = 3
        for i in range(retries):
            try:
                process_output = subprocess.check_output(
                    [f'{self.duckdb_path}/build/release/benchmark/benchmark_runner PartitioningBenchmark'],
                    shell=True, universal_newlines=True, stderr=subprocess.STDOUT)
            except subprocess.CalledProcessError as grepexc:
                self.logger.error("Error while calling benchmark runner, code" + str(grepexc.returncode) + str(grepexc.output))
                if i < retries - 1:  # i is zero indexed
                    self.logger.warning("Retrying...")
                    continue
                else:
                    raise Exception(str(grepexc.output))
            break
        result_rows = str(process_output).split('\n')
        latencies = []
        if "ValueOrDie called on an error:" in result_rows or "Aborted" in result_rows:
            self.logger.error(f"Error while running benchmarks: {result_rows}")
        for row in result_rows[1:]:
            try:
                str_value = row.split('\t')[-1]
                if str_value != "" and "Segmentation" not in str_value:
                    latency = float(str_value)
                    latencies.append(latency)
            except Exception as e:
                self.logger.error(f"Error while parsing benchmark results: {str(e)}")
        try:
            num_partitions_filename = os.path.join(self.duckdb_path, PARTITIONS_LOG_FILE)
            with open(num_partitions_filename, 'r') as num_partitions_file:
                used_partitions = int(num_partitions_file.read())
        except Exception as e:
            self.logger.error(f"Could not load number of used partitions from the log file - {str(e)}")
            used_partitions = 0
        try:
            average_partition_size = 0
            partitions_folder = self.benchmark.get_dataset_folder(self.config.partitioning)
            partition_files = [os.path.join(partitions_folder, f)
                               for f in os.listdir(partitions_folder) if f.endswith(".parquet")]
            if len(partition_files) == 0:
                self.logger.error(f"No partitions in the folder {partitions_folder}")
                raise Exception
            for partition_file in partition_files:
                average_partition_size += os.path.getsize(partition_file)
            average_partition_size /= len(partition_files)
            TO_MB = 1024 * 1024
            average_partition_size /= TO_MB
            average_partition_size = round(average_partition_size, 2)
        except Exception as e:
            self.logger.warning("Could not compute the average partition size in bytes")
            average_partition_size = 0
        self.result = BenchmarkResult(self.benchmark, self.config, latencies, used_partitions, average_partition_size)
        self.logger.info("Collected results from benchmark execution")

    def collect_results(self):
        with open(self.config.results_file, 'a') as result_file:
            result_file.write(self.result.format())
        self.logger.info(f'Completed run for benchmark {self.config}, results saved to {self.config.results_file}')

    def cleanup(self):
        # TODO: support deletion from remote object store (e.g. S3) at some point
        StorageManager.delete_files(self.benchmark.get_dataset_folder(self.config.partitioning))
        self.logger.info(
            f'Removed parquet files from folder {self.benchmark.get_dataset_folder(self.config.partitioning)}')

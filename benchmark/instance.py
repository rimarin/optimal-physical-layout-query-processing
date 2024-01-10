import logging
import os
import re
import subprocess

from config import BenchmarkConfig
from exceptions import BenchmarkRunnerException
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
            process = subprocess.run(partitioner_command)
            if process.returncode != 0:
                self.logger.error(f'Received return code {str(process.returncode)}')
        except Exception as e:
            self.logger.error(f'Partitioning failed: {str(e)}')

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
        def build_benchmarks():
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

        def generate_temp_query_file(tmp_query_file_path):
            tmp_folder = os.path.join(os.path.abspath(os.getcwd()), 'temp')
            if not os.path.exists(tmp_folder):
                os.makedirs(tmp_folder)
            generated_query_file = f"{self.benchmark.get_generated_queries_folder()}/{self.query_number}{self.query_variant}.sql"
            subprocess.check_output(f"cp {generated_query_file} {tmp_folder}", shell=True)
            moved_query_file = os.path.join(tmp_folder, f'{self.query_number}{self.query_variant}.sql')
            subprocess.check_output(f"mv {moved_query_file} {tmp_query_file_path}", shell=True)

        def replace_from_clause(tmp_query_file_path):
            with open(tmp_query_file_path, 'r') as tmp_query_file:
                original_query = tmp_query_file.read()
            from_clause = f'FROM read_parquet(\'{self.benchmark.get_dataset_folder(self.config.partitioning)}/*{DATA_FORMAT}\')'
            tmp_query = re.sub('FROM?(.*?)where', f'{from_clause} where', original_query, flags=re.DOTALL)
            with open(tmp_query_file_path, 'w') as tmp_query_file:
                tmp_query_file.write(tmp_query)

        build_benchmarks()
        tmp_query_path = os.path.join(self.duckdb_path, "query.sql")
        generate_temp_query_file(tmp_query_path)
        replace_from_clause(tmp_query_path)

    def runner_launch(self):

        def launch_duckdb_benchmark():
            max_retries = 5
            process_output = None
            for i in range(max_retries):
                try:
                    self.logger.info("Launching benchmarks...")
                    cmd = [f'{self.duckdb_path}/build/release/benchmark/benchmark_runner', 'PartitioningBenchmark']
                    process = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                    if process.returncode != 0:
                        self.logger.error(f"Received return code {str(process.returncode)}")
                        raise Exception("Benchmark did not succeed")
                    process_output = process.stderr.decode("utf-8")
                    if len(str(process_output).split('\n')) < 3:
                        raise Exception("Process output is too short")
                except Exception as e:
                    self.logger.error(f"Error while calling benchmark runner, {str(e)}")
                    if i < max_retries - 1:  # i is zero indexed
                        self.logger.warning("Retrying...")
                        continue
                    else:
                        self.logger.error(f'Even after {max_retries} could not make it work {str(e)} :(')
                        raise BenchmarkRunnerException(f'Failed after {max_retries}')
                break
            return process_output

        def parse_benchmark_results(output_results):
            if "ValueOrDie called on an error:" in output_results or "Aborted" in output_results:
                self.logger.error(f"Error while running benchmarks: {output_results}")
            result_rows = str(output_results).split('\n')
            parsed_latencies = []
            for row in result_rows[1:]:
                try:
                    str_value = row.split('\t')[-1]
                    if str_value != "" and "Segmentation" not in str_value:
                        latency = float(str_value)
                        parsed_latencies.append(latency)
                except Exception as e:
                    self.logger.error(f"Error while parsing benchmark results: {str(e)}")
            return parsed_latencies

        def parse_num_used_partitions():
            parsed_used_partitions = 0
            try:
                num_partitions_filename = os.path.join(self.duckdb_path, PARTITIONS_LOG_FILE)
                with open(num_partitions_filename, 'r') as num_partitions_file:
                    parsed_used_partitions = int(num_partitions_file.read())
            except Exception as e:
                self.logger.error(f"Could not load number of used partitions from the log file - {str(e)}")
            return parsed_used_partitions

        def get_partition_size():
            avg_partition_size = 0
            try:
                partitions_folder = self.benchmark.get_dataset_folder(self.config.partitioning)
                partition_files = [os.path.join(partitions_folder, f)
                                   for f in os.listdir(partitions_folder) if f.endswith(".parquet")]
                if len(partition_files) == 0:
                    self.logger.error(f"No partitions in the folder {partitions_folder}")
                    raise Exception
                for partition_file in partition_files:
                    avg_partition_size += os.path.getsize(partition_file)
                avg_partition_size /= len(partition_files)
                to_mb = 1024 * 1024
                avg_partition_size /= to_mb
                avg_partition_size = round(avg_partition_size, 2)
            except Exception as e:
                self.logger.warning("Could not compute the average partition size in bytes")
            return avg_partition_size

        benchmark_str_results = launch_duckdb_benchmark()
        latencies = parse_benchmark_results(benchmark_str_results)
        used_partitions = parse_num_used_partitions()
        average_partition_size = get_partition_size()
        self.result = BenchmarkResult(self, latencies, used_partitions, average_partition_size)
        self.logger.info("Collected results from benchmark execution")

    def collect_results(self):
        with open(self.config.results_file, 'a') as result_file:
            result_file.write(self.result.format())
        self.logger.info(f'Completed run for benchmark {self.config}, results saved to {self.config.results_file}')

    def cleanup(self):
        StorageManager.delete_files(self.benchmark.get_dataset_folder(self.config.partitioning))
        self.logger.info(
            f'Removed parquet files from folder {self.benchmark.get_dataset_folder(self.config.partitioning)}')

import logging
import os
import re
import subprocess
import time

from benchmarks.osm import BenchmarkOSM
from benchmarks.taxi import BenchmarkTaxi
from benchmarks.tpch import BenchmarkTPCH
from config import BenchmarkConfig
from exceptions import BenchmarkRunnerException
from result import BenchmarkResult
from settings import DATA_FORMAT, PARTITIONS_LOG_FILE, RESULTS_LOG_FILE, ROW_GROUPS_LOG_FILE, ROWS_LOG_FILE
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
            start_time = time.time()
            process = subprocess.run(partitioner_command)
            time_to_partition = int(time.time() - start_time)
            self.config.time_to_partition = time_to_partition
            self.logger.info(f'Partitioner took {time_to_partition} seconds')
            if process.returncode != 0:
                self.logger.error(f'Received return code {str(process.returncode)}')
                self.logger.error(f'Error details: {str(process.stderr)}')
        except Exception as e:
            self.logger.error(f'Partitioning failed: {str(e)}')

        try:
            self.config.total_partitions = self.benchmark.get_num_total_partitions(self.config.partitioning)
        except Exception as e:
            self.logger.error(f'Could not get the total number of partitions - {str(e)}')
            self.config.total_partitions = 0
        if self.config.total_partitions == 0:
            self.logger.warning('No partitions, something could be wrong. Used command:')
            self.logger.warning(f'{" ".join(partitioner_command)}')

    @staticmethod
    def get_benchmark(name):
        """
        Retrieve the Benchmark object according to the provided name
        """
        scale = 10
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
                    cmd = [f'{self.duckdb_path}/build/release/benchmark/benchmark_runner', 'PartitioningBenchmark',
                           f'--out={RESULTS_LOG_FILE}']
                    timeout = 180  # s
                    process = subprocess.run(cmd, timeout=timeout)
                    if process.returncode != 0:
                        self.logger.error(f"Received return code {str(process.returncode)}")
                        raise Exception("Benchmark did not succeed")
                except Exception as e:
                    self.logger.error(f"Error while calling benchmark runner, {str(e)}")
                    if i < max_retries - 1:
                        self.logger.warning("Retrying...")
                        continue
                    else:
                        self.logger.error(f'Even after {max_retries} could not make it work {str(e)} :(')
                        raise BenchmarkRunnerException(f'Failed after {max_retries}')
                break
            return process_output

        def parse_benchmark_results():
            parsed_latencies = []
            results_file_path = os.path.join(self.duckdb_path, RESULTS_LOG_FILE)
            if os.path.isfile(results_file_path):
                results_file = open(results_file_path, 'r')
                results_lines = results_file.readlines()
                for line in results_lines:
                    try:
                        line = line.strip()
                        if line != "" and "Segmentation" not in line:
                            latency = float(line)
                            parsed_latencies.append(latency)
                    except Exception as e:
                        self.logger.warning(f"Could not parse benchmark results: {str(e)}")
            else:
                self.logger.error(f"Results log file does not exist: {results_file_path}")
            return parsed_latencies

        def parse_num_from_file(filename):
            parsed_num = 0
            try:
                filepath = os.path.join(self.duckdb_path, filename)
                with open(filepath, 'r') as file:
                    parsed_num = int(file.read())
            except Exception as e:
                self.logger.error(f"Could not load number from file {filename} - {str(e)}")
            return parsed_num

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

        launch_duckdb_benchmark()
        latencies = parse_benchmark_results()
        fetched_partitions = parse_num_from_file(PARTITIONS_LOG_FILE)
        fetched_row_groups = parse_num_from_file(ROW_GROUPS_LOG_FILE)
        fetched_rows = parse_num_from_file(ROWS_LOG_FILE)
        average_partition_size = get_partition_size()
        self.result = BenchmarkResult(self, latencies, average_partition_size, fetched_partitions, fetched_row_groups,
                                      fetched_rows)
        self.logger.info("Collected results from benchmark execution")

    def collect_results(self):
        try:
            with open(self.config.results_file, 'a') as result_file:
                result_file.write(self.result.format())
            self.logger.info(f'Completed run for benchmark {self.config}, results saved to {self.config.results_file}')
        except Exception as e:
            self.logger.error(f"Could not collect results from benchmark - {str(e)}")

    def cleanup(self):
        dataset_folder = self.benchmark.get_dataset_folder(self.config.partitioning)
        try:
            StorageManager.delete_files(dataset_folder)
            self.logger.info(
                f'Removed parquet files from folder {dataset_folder}')
        except Exception as e:
            self.logger.error(f"Could not delete parquet files from folder {dataset_folder} - {str(e)}")

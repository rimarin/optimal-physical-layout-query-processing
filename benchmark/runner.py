import logging
import os
import sys

from config import BenchmarkConfig
from instance import BenchmarkInstance
from result import BenchmarkResult
from settings import RESULTS_FILE, PARTITIONINGS, PARTITION_SIZES, DATASETS, LOG_TO_CONSOLE, LOG_TO_FILE, RESULTS_FOLDER


def run_benchmarks(datasets: list, partitionings: list, partition_sizes: list):
    def initialize_logger():
        """
        Setup logger to log events to stdout or file, depending on the configuration
        """
        logger = logging.getLogger()
        logger.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s | %(levelname)s | %(message)s')
        if LOG_TO_CONSOLE:
            stdout_handler = logging.StreamHandler(sys.stdout)
            stdout_handler.setLevel(logging.DEBUG)
            stdout_handler.setFormatter(formatter)
            logger.addHandler(stdout_handler)
        if LOG_TO_FILE:
            file_handler = logging.FileHandler('benchmark_runner.log')
            file_handler.setLevel(logging.DEBUG)
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
        return logger

    def initialize_results_file():
        """
        Write out the csv header of the results file
        """
        if not os.path.exists(RESULTS_FOLDER):
            os.makedirs(RESULTS_FOLDER)
        if not os.path.exists(RESULTS_FILE):
            with open(RESULTS_FILE, 'w+') as result_file:
                result_file.write(BenchmarkResult.format_header())

    logger = initialize_logger()
    initialize_results_file()
    for dataset in datasets:
        try:
            benchmark = BenchmarkInstance.get_benchmark(dataset)
            query_files = BenchmarkInstance.get_query_files(benchmark)
            partitioning_columns_groups = benchmark.get_partitioning_columns()
        except Exception as e:
            logger.error(f"Error while preparing benchmark instance for dataset name {dataset} - " + str(e))
            continue
        for partitioning in partitionings:
            for partition_size in partition_sizes:
                for partitioning_columns in partitioning_columns_groups:
                    for i, query_file in enumerate(query_files):
                        num_query, query_variant = BenchmarkInstance.get_query_num_and_variant(query_file)
                        try:
                            benchmark_instance = BenchmarkInstance(BenchmarkConfig(
                                dataset=dataset,
                                partitioning=partitioning,
                                partition_size=partition_size,
                                query_number=num_query,
                                query_variant=query_variant,
                                partitioning_columns=partitioning_columns,
                                results_file=RESULTS_FILE
                            ), logger)
                            benchmark_instance.prepare_dataset()
                            benchmark_instance.prepare_queries()
                            if i == 0:
                                benchmark_instance.generate_partitions()
                            benchmark_instance.runner_prepare()
                            benchmark_instance.runner_launch()
                            benchmark_instance.collect_results()
                        except Exception as e:
                            logger.error(f"Error while running benchmark instance with settings: "
                                         f"{dataset}-{partitioning}-{partition_size}-{num_query}-{query_variant}-"
                                         f"{partitioning_columns} - " + str(e))
                            continue
                    benchmark_instance.cleanup()

if __name__ == "__main__":
    run_benchmarks(DATASETS, PARTITIONINGS, PARTITION_SIZES)

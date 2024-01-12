import logging
import os
import sys

from config import BenchmarkConfig
from instance import BenchmarkInstance
from result import BenchmarkResult
from settings import RESULTS_FILE, PARTITIONINGS, PARTITION_SIZES, DATASETS, LOG_TO_CONSOLE, LOG_TO_FILE, \
    RESULTS_FOLDER, NUM_QUERIES


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

    def initialize_debug_info():
        logger.info("== Data layout partitioning benchmark ==")
        total_layouts = 0
        total_queries = 0
        avg_query_latency = 2
        avg_partitioning_time = 60 * 30
        for _dataset in datasets:
            _benchmark = BenchmarkInstance.get_benchmark(_dataset)
            dataset_layouts = len(partitionings) * len(partition_sizes) * len(_benchmark.get_partitioning_columns())
            total_layouts += dataset_layouts
            total_queries += NUM_QUERIES.get(_dataset, 0) * dataset_layouts
        total_time = total_layouts * avg_partitioning_time + total_queries * avg_query_latency
        total_time_hours = int(total_time / 3600)
        total_time_days = int(total_time / 86400)
        logger.info(f"Expecting {total_layouts} layouts to be generated")
        logger.info(f"Expecting {total_queries} queries to be run")
        logger.info(f"Expected time {total_time_hours} hours / {total_time_days} days")
        return total_layouts

    logger = initialize_logger()
    initialize_results_file()
    num_total_layouts = initialize_debug_info()
    generated_layouts = 0
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
                    benchmark_instance = BenchmarkInstance(BenchmarkConfig(
                        dataset=dataset,
                        partitioning=partitioning,
                        partition_size=partition_size,
                        partitioning_columns=partitioning_columns,
                        results_file=RESULTS_FILE
                    ), logger)
                    benchmark_instance.generate_partitions()
                    generated_layouts += 1
                    logger.info(f"Generated layout #{generated_layouts} (out of {num_total_layouts})")
                    for i, query_file in enumerate(query_files):
                        try:
                            query_number, query_variant = BenchmarkInstance.get_query(query_file)
                        except Exception as e:
                            logger.error(f"Cannot parse query number and variant from file name: {str(e)}")
                            continue
                        try:
                            benchmark_instance.set_query(query_number, query_variant)
                            benchmark_instance.prepare_dataset()
                            benchmark_instance.prepare_queries()
                            benchmark_instance.runner_prepare()
                            benchmark_instance.runner_launch()
                            benchmark_instance.collect_results()
                        except Exception as e:
                            logger.error(f"Error while running benchmark instance with settings: "
                                         f"{dataset}-{partitioning}-{partition_size}-{query_number}-{query_variant}-"
                                         f"{partitioning_columns} - " + str(e))
                    benchmark_instance.cleanup()


if __name__ == "__main__":
    run_benchmarks(DATASETS, PARTITIONINGS, PARTITION_SIZES)

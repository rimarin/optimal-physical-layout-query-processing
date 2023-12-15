import logging
import os
import sys

from itertools import combinations

from benchmark_config import BenchmarkConfig
from benchmark_instance import BenchmarkInstance
from benchmark_result import BenchmarkResult
from config import RESULTS_FILE, PARTITIONINGS, PARTITION_SIZES, DATASETS, LOG_TO_CONSOLE, LOG_TO_FILE


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
        if not os.path.exists(RESULTS_FILE):
            with open(RESULTS_FILE, 'w') as result_file:
                result_file.write(BenchmarkResult.format_header())

    logger = initialize_logger()
    initialize_results_file()
    for dataset in datasets:
        for partitioning in partitionings:
            for partition_size in partition_sizes:
                try:
                    benchmark = BenchmarkInstance.get_benchmark(dataset)
                except Exception as e:
                    logger.error(f"Error while retrieving benchmark instance for dataset name {dataset} - " + str(e))
                    continue
                query_files = [f for f in os.listdir(benchmark.get_generated_queries_folder())
                               if f.endswith(".sql")]
                columns = benchmark.get_relevant_columns()
                min_num_dimensions = 2
                max_num_dimensions = len(columns)
                columns_combinations = sum([list(map(list, combinations(columns, i)))
                                            for i in range(min_num_dimensions, max_num_dimensions + 1)], [])
                for columns_combination in columns_combinations:
                    for i, query_file in enumerate(query_files):
                        query_file_name = query_file.split('.sql')[0]
                        num_query = int(''.join(filter(str.isdigit, query_file_name)))
                        query_variant = ''.join(filter(str.isalpha, query_file_name))
                        try:
                            benchmark_instance = BenchmarkInstance(BenchmarkConfig(
                                dataset=dataset,
                                partitioning=partitioning,
                                partition_size=partition_size,
                                query_number=num_query,
                                query_variant=query_variant,
                                partitioning_columns=columns_combination,
                                results_file=RESULTS_FILE
                            ), logger)
                            benchmark_instance.prepare_dataset()
                            benchmark_instance.prepare_queries()
                            if i == 0:
                                benchmark_instance.generate_partitions()
                            benchmark_instance.run()
                        except Exception as e:
                            logger.error(f"Error while running benchmark instance with settings: "
                                         f"{dataset}-{partitioning}-{partition_size}-{num_query}-{query_variant}-"
                                         f"{columns_combination} - " + str(e))
                            continue
                    benchmark_instance.cleanup()


if __name__ == "__main__":
    run_benchmarks(DATASETS, PARTITIONINGS, PARTITION_SIZES)

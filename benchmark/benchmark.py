import duckdb
import os

from workload_taxi import TaxiWorkload
from workload_tpch import TPCHWorkload

"""
== Benchmarking ==
- we are going to use the benchmarking suite including in DuckDB
https://github.com/duckdb/duckdb/tree/main/benchmark#run-a-single-benchmark
- it can automatically load datasets such as: TPC-H, ClickBench, IMDB
- build benchmark runner
- 

"""


def run_benchmarks():

    tpch = TPCHWorkload()
    if not tpch.is_dataset_generated():
        tpch.generate_dataset()
    queries = tpch.generate_queries()

    taxi = TaxiWorkload()
    if not taxi.is_dataset_generated():
        taxi.generate_dataset()


if __name__ == "__main__":
    run_benchmarks()

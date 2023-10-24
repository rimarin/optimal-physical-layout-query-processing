import duckdb
import os

from workload_osm import OSMWorkload
from workload_taxi import TaxiWorkload
from workload_tpch import TPCHWorkload

"""
== Benchmarking ==
- we are going to use the benchmarking suite including in DuckDB
https://github.com/duckdb/duckdb/tree/main/benchmark#run-a-single-benchmark
- it can automatically load datasets such as: TPC-H, ClickBench, IMDB
- build benchmark runner
- build/release/benchmark/benchmark_runner benchmark/tpch/sf1-parquet/q03.benchmark 

"""


def run_benchmarks():

    taxi = TaxiWorkload()
    if not taxi.is_dataset_generated():
        taxi.generate_dataset()
    taxi.generate_queries()

    # tpch = TPCHWorkload()
    # if not tpch.is_dataset_generated():
    #    tpch.generate_dataset()
    # queries = tpch.generate_queries()

    # osm = OSMWorkload()
    # if not osm.is_dataset_generated():
    #    osm.generate_dataset()
    # queries = osm.generate_queries()


if __name__ == "__main__":
    run_benchmarks()

import duckdb
import os

from workload import Workload
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


def prepare_data_and_queries():

    taxi = TaxiWorkload()
    if not taxi.is_dataset_generated():
        taxi.generate_dataset()
    if not taxi.is_query_workload_generated():
        taxi.generate_queries()

    tpch = TPCHWorkload()
    if not tpch.is_dataset_generated():
        tpch.generate_dataset()
    if not tpch.is_query_workload_generated():
        tpch.generate_queries()

    osm = OSMWorkload()
    if not osm.is_dataset_generated():
        osm.generate_dataset()
    if not osm.is_query_workload_generated():
        osm.generate_queries()


def mass_rename_queries():
    taxi = TaxiWorkload()
    tpch = TPCHWorkload()
    osm = OSMWorkload()
    Workload.rename_queries(taxi.get_generated_queries_folder())
    Workload.rename_queries(tpch.get_generated_queries_folder())
    Workload.rename_queries(osm.get_generated_queries_folder())


def run_benchmarks():
    duckdb_folder = "../duckdb"
    duckdb_path = os.path.join(os.path.abspath(os.getcwd()), duckdb_folder)
    if not os.path. exists(duckdb_path):
        os.system("git clone https://github.com/duckdb/duckdb")
        os.system(f"cd {duckdb_path}")
    os.system("BUILD_BENCHMARK=1 BUILD_TPCH=1 make")
    os.system(f'build/release/benchmark/benchmark_runner "benchmark/osm/.*"')
    os.system(f'build/release/benchmark/benchmark_runner "benchmark/taxi/.*"')
    os.system(f'build/release/benchmark/benchmark_runner "benchmark/tpch/.*"')


if __name__ == "__main__":
    # prepare_data_and_queries()
    # mass_rename_queries()
    run_benchmarks()

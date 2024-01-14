from instance import BenchmarkInstance
from settings import TAXI, OSM, TPCH_SF10

if __name__ == "__main__":
    for dataset in [TPCH_SF10]:
        benchmark = BenchmarkInstance.get_benchmark(dataset)
        benchmark.generate_queries()

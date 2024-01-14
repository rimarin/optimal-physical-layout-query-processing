from instance import BenchmarkInstance
from settings import TAXI, OSM


if __name__ == "__main__":
    for dataset in [OSM]:
        benchmark = BenchmarkInstance.get_benchmark(dataset)
        benchmark.generate_queries()

import datetime
import duckdb
import os
import random
import re
import requests

from benchmark import Benchmark
from settings import DATA_FORMAT


class BenchmarkTaxi(Benchmark):

    def __init__(self):
        super().__init__()
        self.total_rows = None
        self.start_year = 2018
        self.end_year = 2019

    def get_name(self):
        return 'taxi'

    @staticmethod
    def get_partitioning_columns():
        return [
            ["PULocationID"],
            ["PULocationID", "DOLocationID"],
            ["PULocationID", "DOLocationID", "tpep_pickup_datetime"],
            ["PULocationID", "DOLocationID", "tpep_pickup_datetime", "tpep_dropoff_datetime"],
            ["PULocationID", "DOLocationID", "tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count"],
            ["PULocationID", "DOLocationID", "tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count", "fare_amount"],
            ["PULocationID", "DOLocationID", "tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count", "fare_amount", "trip_distance"]
        ]

    def get_schema(self):
        return {
            "VendorID": int,
            "tpep_pickup_datetime": int,
            "tpep_dropoff_datetime": int,
            "passenger_count": int,
            "trip_distance": float,
            "RatecodeID": int,
            "store_and_fwd_flag": str,
            "PULocationID": int,
            "DOLocationID": int,
            "payment_type": int,
            "extra": float,
            "mta_tax": float,
            "tip_amount": float,
            "tolls_amount": float,
            "improvement_surcharge": float,
            "total_amount": float,
            "congestion_surcharge": float,
            "airport_fee": float,
        }

    def generate_dataset(self, **params):
        self.start_year = params.get("start_year", self.start_year)
        self.end_year = params.get("end_year", self.end_year)
        years = [str(year) for year in range(self.start_year, self.end_year + 1)]
        months = [str(month).zfill(2) for month in range(1, 13)]

        for year in years:
            for month in months:
                filename = os.path.join(self.get_dataset_folder(), f'trips_{year}-{month}.parquet')
                with open(filename, 'wb') as out_file:
                    url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month}.parquet'
                    print(f'Requesting file: {url}')
                    content = requests.get(url, stream=True).content
                    out_file.write(content)

        self.total_rows = len(duckdb.sql(f'SELECT * FROM read_parquet(\'{self.get_files_pattern()}\')'))

    def generate_queries(self):
        """
        Approach resembles that in Tsunami paper:
        1. Generate uniform number from 1 to 6 to pick the partitioning_columns
        2. Uniformly pick a value within the range of each column
        However, we do not need the first step, since our query templates are already designed to have an increasing
        number of dimensions.
        """
        start_date = datetime.datetime(2018, 1, 1)
        end_date = datetime.datetime(2019, 12, 31)
        value_ranges = {
            'PULocationID': [1, 265],
            'DOLocationID': [1, 265],
            'tpep_pickup_datetime': [start_date, end_date],
            'tpep_dropoff_datetime': [start_date, end_date],
            'passenger_count': [1, 8],
            'fare_amount': [0, 500],
            'trip_distance': [1, 50],
        }
        placeholders = {
            '1': 'PULocationID',
            '2': 'PULocationID',
            '3': 'DOLocationID',
            '4': 'DOLocationID',
            '5': 'tpep_pickup_datetime',
            '6': 'tpep_pickup_datetime',
            '7': 'tpep_dropoff_datetime',
            '8': 'tpep_dropoff_datetime',
            '9': 'passenger_count',
            '10': 'passenger_count',
            '11': 'fare_amount',
            '12': 'fare_amount',
            '13': 'trip_distance',
            '14': 'trip_distance'
        }
        templates = [_ for _ in range(1, 7 + 1)]
        num_queries_per_template = 500
        for template in templates:
            with open(os.path.join(self.get_queries_folder(), f'{str(template)}.sql'), 'r') as template_file:
                query_template = template_file.read()
            from_clause = f'FROM read_parquet(\'{self.get_dataset_folder()}/*.parquet\')'
            query = re.sub('FROM?(.*?)WHERE', f'{from_clause} where', query_template, flags=re.DOTALL)
            used_placeholders = re.findall('\':(\d+)\'', query)
            for i in range(num_queries_per_template):
                final_query = query[:]
                for used_placeholder in used_placeholders:
                    value_range = value_ranges[placeholders[used_placeholder]]
                    range_start, range_end = value_range[0], value_range[1]
                    if isinstance(range_start, datetime.datetime):
                        random_value = ("'" + (range_start + (range_end - range_start) * random.random())
                                        .strftime("%Y%m%d%H:%M:%S") + "'")
                    else:
                        random_value = random.uniform(range_start, range_end)
                    final_query = re.sub(f'\':{used_placeholder}\'', f'{random_value}', final_query, flags=re.DOTALL)
                print(f'Executing query: {final_query}')
                try:
                    num_tuples = len(duckdb.sql(final_query))
                except Exception as e:
                    print(f"Error {str(e)} while executing query: {final_query}")
                    continue
                rounding_digits = 4
                query_selectivity = round((num_tuples / self.get_total_rows()) * 100, rounding_digits)
                min_selectivity, max_selectivity = 0.001, 10
                if query_selectivity != 0 and min_selectivity < query_selectivity < max_selectivity:
                    with open(os.path.join(self.get_generated_queries_folder(), f'{str(template)}_{str(query_selectivity)}.sql'),
                              'w') as query_file:
                        query_file.write(final_query)

    def rename_files_to_rounded(self):
        digits = 4
        for f in os.listdir(self.get_generated_queries_folder()):
            if not f.startswith('.'):
                num = float(f.split('_')[1].split('.sql')[0])
                os.rename(os.path.join(self.get_generated_queries_folder(), f),
                          os.path.join(self.get_generated_queries_folder(), f.replace(str(num),
                                                                                      str(round(num, digits)))))

    def is_dataset_generated(self) -> bool:
        taxi_file = os.path.join(self.get_dataset_folder(), f'{self.get_name()}{DATA_FORMAT}')
        return os.path.exists(taxi_file)

    def is_query_workload_generated(self) -> bool:
        # TODO: implement
        return any(f.endswith(".sql") for f in os.listdir(self.get_generated_queries_folder()))


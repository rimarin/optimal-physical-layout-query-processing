import datetime
import duckdb
import os
import random
import re

from workload import Workload


class OSMWorkload(Workload):

    def __init__(self):
        super().__init__()
        self.total_rows = None

    def get_name(self):
        return 'osm'

    def get_table_name(self):
        return ''

    def generate_dataset(self, **params):
        os.system(f"aws s3 cp s3://daylight-openstreetmap/parquet/osm_features/release=v1.33/type=node/ "
                  f"{self.get_dataset_folder()} --recursive")
        for f in os.listdir(self.get_dataset_folder()):
            os.rename(os.path.join(self.get_generated_queries_folder(), f),
                      os.path.join(self.get_generated_queries_folder(), f + '.parquet'))
        self.total_rows = len(duckdb.sql(f'SELECT * FROM read_parquet(\'{self.get_files_pattern()}\')'))

    def generate_queries(self):
        start_date = datetime.datetime(2006, 1, 22)
        end_date = datetime.datetime(2023, 10, 12)
        value_ranges = {
            'min_lon': [-180, 180],
            'max_lon': [-180, 180],
            'min_lat': [-90, 90],
            'max_lat': [-90, 90],
            'created_at': [start_date, end_date],
            'version': [1, 10],
            'id': [1, 11258692953],
        }
        placeholders = {
            '1': 'min_lon',
            '2': 'min_lon',
            '3': 'max_lon',
            '4': 'max_lon',
            '5': 'min_lat',
            '6': 'min_lat',
            '7': 'max_lat',
            '8': 'max_lat',
            '9': 'created_at',
            '10': 'created_at',
            '11': 'version',
            '12': 'version',
            '13': 'id',
            '14': 'id'
        }
        templates = [_ for _ in range(1, 7 + 1)]
        num_queries_per_template = 500
        for template in templates:
            with open(os.path.join(self.get_queries_folder(), f'{str(template)}.sql'), 'r') as template_file:
                query_template = template_file.read()
            from_clause = f'FROM read_parquet(\'{self.get_dataset_folder()}/{self.get_table_name()}*.parquet\')'
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
                min_selectivity, max_selectivity = 0.001, 5
                if query_selectivity != 0 and min_selectivity < query_selectivity < max_selectivity:
                    with open(os.path.join(self.get_generated_queries_folder(), f'{str(template)}_{str(query_selectivity)}.sql'),
                              'w') as query_file:
                        query_file.write(final_query)

    def is_dataset_generated(self) -> bool:
        return any(f.endswith(".parquet") for f in os.listdir(self.get_dataset_folder()))

    def is_query_workload_generated(self) -> bool:
        return False

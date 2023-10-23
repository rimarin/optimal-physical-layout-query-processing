import duckdb
import os
import re
import requests

from workload import Workload


class TaxiWorkload(Workload):

    def __init__(self):
        super().__init__()
        self.total_rows = None
        self.start_year = 2018
        self.end_year = 2019

    def get_name(self):
        return 'taxi'

    def get_table_name(self):
        return 'trips'

    def generate_dataset(self, **params):
        self.start_year = params.get("start_year", self.start_year)
        self.end_year = params.get("end_year", self.end_year)
        years = [str(year) for year in range(self.start_year, self.end_year+1)]
        months = [str(month).zfill(2) for month in range(1, 13)]

        for year in years:
            for month in months:
                filename = f'trips_{year}-{month}.parquet'
                with open(filename, 'wb') as out_file:
                    url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/{filename}'
                    print(f'Requesting file: {url}')
                    content = requests.get(url, stream=True).content
                    out_file.write(content)

        self.total_rows = len(duckdb.sql(f'SELECT * FROM read_parquet(\'{self.get_files_pattern()}\')'))

    def generate_queries(self):
        t1 = "SELECT COUNT(*) FROM trips WHERE Start_Lon > X AND Start_Lon < Y AND Start_Lat > Y AND Start_Lat < Y"
        from_clause = f'FROM read_parquet(\'{self.get_dataset_folder()}/{self.get_table_name()}*.parquet\')'
        query = re.sub('from?(.*?)where', f'{from_clause} where', t1, flags=re.DOTALL)
        num_tuples = len(duckdb.sql(query))
        selectivity = num_tuples / self.total_rows
        with open(self.get_queries_folder() + f'{str(template)}_{str(selectivity)}.sql', 'a') as query_file:
            query_file.write(query)

    def is_dataset_generated(self) -> bool:
        first_file = os.path.join(self.get_dataset_folder(), self.get_table_name() + f'_{self.start_year}-01.parquet')
        last_file = os.path.join(self.get_dataset_folder(), self.get_table_name() + f'_{self.end_year}-12.parquet')
        return os.path.exists(first_file) and os.path.exists(last_file)

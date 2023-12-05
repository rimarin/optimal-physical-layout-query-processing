import duckdb
import os
import re
import subprocess

from workload import Workload


class TPCHWorkload(Workload):

    def __init__(self, scale = 1):
        super().__init__()
        self.total_rows = None
        self.scale = scale

    def get_name(self):
        return f'tpch-sf{self.scale}'

    def get_relevant_columns(self):
        return ["PULocationID", "DOLocationID", "tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count",
                "fare_amount", "trip_distance"]

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

    def get_table_name(self):
        return 'lineitem'

    def generate_dataset(self, **params):
        """
        Generate the TPC-H benchmark database from DuckDB
        https://duckdb.org/docs/extensions/tpch.html
        https://duckdb.org/docs/sql/statements/export.html
        """
        # Install/load the tpch extension
        duckdb.sql('INSTALL tpch')
        duckdb.sql('LOAD tpch')

        # Generate data for scale factor 1, use:
        self.scale = params.get("scale", 1)
        duckdb.sql(f'CALL dbgen(sf={self.scale})')

        # Denormalize database into a big, joined fact table lineitem
        # Size should approximately increase by x7 factor
        duckdb.sql('CREATE TABLE lineitem_denorm AS '
                   'SELECT * '
                   'FROM customer, lineitem, nation, orders, part, partsupp, region, supplier '
                   'WHERE '
                   'c_custkey = o_custkey AND '
                   'n_nationkey = c_nationkey AND '
                   'n_nationkey = s_nationkey AND '
                   'o_orderkey = l_orderkey AND '
                   'p_partkey = ps_partkey AND '
                   'ps_partkey = l_partkey AND '
                   'ps_suppkey = l_suppkey AND '
                   'r_regionkey = n_regionkey AND '
                   's_suppkey = ps_suppkey;')

        # Export the entire table to a parquet file
        database_file = os.path.join(self.get_dataset_folder(), 'tpch.parquet')
        duckdb.sql(f'COPY (SELECT * FROM lineitem_denorm) TO \'{database_file}\' (FORMAT PARQUET)')
        self.total_rows = len(duckdb.sql(f'SELECT * FROM lineitem_denorm'))

    def generate_queries(self):
        """
        We selected 8 query templates in TPC-H that have relatively selective filter predicates.
        Namely, the queries are: q3, q5, q6, q8, q10, q12, q14 and q19.
        For each template, we generated 10 queries using the TPC-H query generator.
        Use the tpch-dbgen utility (https://github.com/electrum/tpch-dbgen) to generate the tpc-h queries.
        # export DSS_QUERY=PATH_TO_QUERIES_FOLDER
        """
        templates = [3, 5, 6, 10, 12, 14, 19]
        num_queries_per_template = 10

        templates_folder = os.path.join(self.get_queries_folder())
        os.system(f'cd {templates_folder}; export DSS_QUERY={templates_folder}')

        for template in templates:
            for i in range(num_queries_per_template):
                query = subprocess.check_output(f'cd {self.get_queries_folder()};'
                                                f'./qgen {template}', shell=True).decode('utf-8')
                from_clause = f'FROM read_parquet(\'{self.get_files_pattern()}\')'
                query = re.sub('from?(.*?)where', f'{from_clause} where', query, flags=re.DOTALL)
                print(f'Executing query: {query}')
                try:
                    num_tuples = len(duckdb.sql(query))
                except Exception as e:
                    print(f"Error {str(e)} while executing query: {query}")
                    continue
                rounding_digits = 4
                query_selectivity = round((num_tuples / self.get_total_rows()) * 100, rounding_digits)
                min_selectivity, max_selectivity = 0, 50
                if query_selectivity != 0 and min_selectivity < query_selectivity < max_selectivity:
                    with open(os.path.join(self.get_generated_queries_folder(), f'{str(template)}_{str(query_selectivity)}.sql'),
                              'w') as query_file:
                        query_file.write(query)

    def is_dataset_generated(self) -> bool:
        return os.path.exists(os.path.join(self.get_dataset_folder(), self.get_table_name() + '.parquet'))

    def is_query_workload_generated(self) -> bool:
        # TODO: implement
        return any(f.endswith(".sql") for f in os.listdir(self.get_generated_queries_folder()))

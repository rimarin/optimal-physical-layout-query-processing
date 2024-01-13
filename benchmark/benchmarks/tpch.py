import duckdb
import os
import re
import subprocess

from benchmark import Benchmark


class BenchmarkTPCH(Benchmark):

    def __init__(self, scale=10):
        super().__init__()
        self.total_rows = None
        self.scale = scale

    def get_name(self):
        return f'tpch-sf{self.scale}'

    @staticmethod
    def get_partitioning_columns():
        return [
            ["c_custkey", "o_orderkey"],
            ["c_custkey", "o_orderkey", "o_orderdate"],
            ["c_custkey", "o_orderkey", "o_orderdate", "l_shipdate"],
        #    ["c_custkey", "o_orderkey", "o_orderdate", "l_shipdate", "s_suppkey"],
        #    ["c_custkey", "o_orderkey", "o_orderdate", "l_shipdate", "s_suppkey", "n_nationkey"],
        #    ["c_custkey", "o_orderkey", "o_orderdate", "l_shipdate", "s_suppkey", "n_nationkey", "r_regionkey"],
        #    ["c_custkey", "o_orderkey", "o_orderdate", "l_shipdate", "s_suppkey", "n_nationkey", "r_regionkey", "l_commitdate"],
        #    ["c_custkey", "o_orderkey", "o_orderdate", "l_shipdate", "s_suppkey", "n_nationkey", "r_regionkey", "l_commitdate", "l_receiptdate"],
        #    ["c_custkey", "o_orderkey", "o_orderdate", "l_shipdate", "s_suppkey", "n_nationkey", "r_regionkey", "l_commitdate", "l_receiptdate", "p_size"],
        #    ["c_custkey", "o_orderkey", "o_orderdate", "l_shipdate", "s_suppkey", "n_nationkey", "r_regionkey", "l_commitdate", "l_receiptdate", "p_size", "p_partkey"]
        ]

    @staticmethod
    def get_query_columns(query_number):
        query_number = str(query_number)
        query_to_cols = {
            '3': ['l_suppkey', 'o_orderkey', 'c_custkey', 's_suppkey', 's_nationkey', 'o_orderdate', 'c_nationkey',
                  'l_orderkey', 'o_custkey', 'n_nationkey', 'n_regionkey', 'r_name', 'r_regionkey'],
            '5': ['o_orderkey', 'c_custkey', 'o_orderdate', 'l_orderkey', 'o_custkey', 'c_mktsegment', 'l_shipdate'],
            '6': ['l_shipdate', 'l_quantity', 'l_discount'],
            '10': ['o_orderkey', 'o_orderdate', 'c_custkey', 'l_returnflag', 'c_nationkey', 'l_orderkey', 'o_custkey',
                   'n_nationkey'],
            '12': ['l_shipmode', 'o_orderkey', 'l_receiptdate', 'l_commitdate', 'l_orderkey', 'l_shipdate'],
            '14': ['p_partkey', 'l_shipdate', 'l_partkey'],
            '19': ['p_partkey', 'p_brand', 'p_container', 'l_quantity', 'l_shipmode', 'l_shipinstruct']
        }
        return query_to_cols.get(query_number, [])

    @staticmethod
    def get_query_selectivity(query):
        query_to_selectivity = {
            "3a": 0.2397,
            "5a": 0.001,
            "6a": 0.0002,
            "10a": 0.7382,
            "12a": 0.0004,
            "14a": 0.0002,
            "19a": 0.0002
        }
        return query_to_selectivity.get(query, 0)

    def get_schema(self):
        raise NotImplementedError

    def generate_dataset(self, **params):
        """
        Generate the TPC-H benchmark database from DuckDB
        https://duckdb.org/docs/extensions/tpch.html
        https://duckdb.org/docs/sql/statements/export.html
        """
        # Install/load the tpch extension
        duckdb.sql('INSTALL tpch')
        duckdb.sql('LOAD tpch')

        # Generate data for scale factor 10, use:
        self.scale = params.get("scale", 10)
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
        subprocess.check_output(f'cd {templates_folder}; export DSS_QUERY={templates_folder}', shell=True)

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

    def get_queries_folder(self):
        return os.path.abspath(os.path.join(self.QUERIES_FOLDER, self.get_name().split('-sf')[0]))

    def is_dataset_generated(self) -> bool:
        return os.path.exists(os.path.join(self.get_dataset_folder(), self.get_name() + '.parquet'))

    def is_query_workload_generated(self) -> bool:
        # TODO: implement
        return any(f.endswith(".sql") for f in os.listdir(self.get_generated_queries_folder()))

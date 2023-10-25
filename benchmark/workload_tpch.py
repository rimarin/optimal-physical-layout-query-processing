import duckdb
import os
import re
import subprocess

from workload import Workload


class TPCHWorkload(Workload):

    def __init__(self):
        super().__init__()
        self.total_rows = None
        self.scale = 1

    def get_name(self):
        return 'tpch'

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
        database_file = os.path.join(self.get_dataset_folder(), 'lineitem.parquet')
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

        for _ in range(num_queries_per_template*len(templates)):
            for template in templates:
                query = subprocess.check_output(f'cd {self.get_queries_folder()};'
                                                f'./qgen {template}', shell=True).decode('utf-8')
                from_clause = f'FROM read_parquet(\'{self.get_files_pattern()}\')'
                query = re.sub('from?(.*?)where', f'{from_clause} where', query, flags=re.DOTALL)
                num_rows = len(duckdb.sql(f'{query}'))
                selectivity = (num_rows / self.get_total_rows()) * 100
                with open(self.get_queries_folder() + f'{str(template)}_{str(selectivity)}.sql', 'a') as query_file:
                    query_file.write(query)

    def is_dataset_generated(self) -> bool:
        return os.path.exists(os.path.join(self.get_dataset_folder(), self.get_table_name() + '.parquet'))
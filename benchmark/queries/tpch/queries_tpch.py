"""
WORKLOAD TPC-H - 100 Queries
We selected ten query templates in TPC-H that have relatively selective filter predicates.
Namely, the queries are: q3, q5, q6, q11, q12, q16, q19, q20, q21 and q22.
The number of columns accessed in these query templates are: 7, 7, 4, 4, 5, 4, 8, 3, 5 and 2, respectively.
For each template, we generated 10 queries using the TPC-H query generator.

Use the tpch dbgen utility (https://github.com/electrum/tpch-dbgen) to generate the tpc-h queries.
Install and initialize it with these steps:

1. git clone https://github.com/electrum/tpch-dbgen.git
2. # In the downloaded directory, edit makefile.suite and set the variables to these values:
CC=gcc
DATABASE=INFORMIX
MACHINE=LINUX
WORKLOAD=TPCH
3. make -f makefile.suite
4. export DSS_QUERY=PATH_TO_QUERIES_FOLDER
"""

import os

templates = [3, 5, 6, 11, 12, 16, 19, 20, 21, 22]
num_queries_per_template = 10
output_file = 'tpch.sql'

os.system(f'export DSS_QUERY=$(pwd)/templates')

i = 1
for template in templates:
    os.system(f'echo "-- == Template {template} =="  >> {output_file}')
    for _ in range(num_queries_per_template):
        os.system(f'echo "-- Query {i}" >> {output_file}')
        os.system(f'./qgen {template} >> {output_file}')
        i += 1

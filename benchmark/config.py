import os

DATASETS = ['tpch-sf1', 'osm', 'taxi']
PARTITIONINGS = ['hilbert-curve', 'kd-tree', 'quad-tree', 'str-tree', 'z-curve-order'] # 'fixed-grid', 'grid-file'
PARTITION_SIZES = [1000, 10000, 50000, 100000, 250000, 500000]

DATA_FORMAT = '.parquet'
RESULTS_FOLDER = 'results'
RESULTS_FILE = os.path.join(RESULTS_FOLDER, 'results.csv')

LOG_TO_CONSOLE = True
LOG_TO_FILE = True

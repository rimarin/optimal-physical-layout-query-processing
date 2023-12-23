import os

TPCH_SF1 = 'tpch-sf1'
TPCH_SF10 = 'tpch-sf10'
OSM = 'osm'
TAXI = 'taxi'
DATASETS = [TPCH_SF1, OSM, TAXI]

NO_PARTITION = 'no-partition'
FIXED_GRID = 'fixed-grid'
GRID_FILE = 'grid-file'
HILBERT_CURVE = 'hilbert-curve'
KD_TREE = 'kd-tree'
QUAD_TREE = 'quad-tree'
STR_TREE = 'str-tree'
Z_ORDER_CURVE = 'z-curve-order'
PARTITIONINGS = [HILBERT_CURVE, KD_TREE, QUAD_TREE, STR_TREE, Z_ORDER_CURVE]
PARTITION_SIZES = [1000, 10000, 50000, 100000, 250000, 500000, 1000000]

DATA_FORMAT = '.parquet'
RESULTS_FOLDER = 'results'
RESULTS_FILE = os.path.join(RESULTS_FOLDER, 'results.csv')
PARTITIONS_LOG_FILE = 'partitions.log'

LOG_TO_CONSOLE = True
LOG_TO_FILE = True

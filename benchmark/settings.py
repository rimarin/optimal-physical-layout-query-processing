import os

TPCH_SF1 = 'tpch-sf1'
TPCH_SF10 = 'tpch-sf10'
TPCH_SF50 = 'tpch-sf50'
TPCH_SF100 = 'tpch-sf100'
OSM = 'osm'
TAXI = 'taxi'
DATASETS = [TPCH_SF100, TAXI, OSM]

NO_PARTITION = 'no-partition'
SCALED = 'scaled'
FIXED_GRID = 'fixed-grid'
GRID_FILE = 'grid-file'
HILBERT_CURVE = 'hilbert-curve'
KD_TREE = 'kd-tree'
QUAD_TREE = 'quad-tree'
STR_TREE = 'str-tree'
Z_ORDER_CURVE = 'z-order-curve'
PARTITIONINGS = [FIXED_GRID, GRID_FILE, KD_TREE, QUAD_TREE, STR_TREE, HILBERT_CURVE, Z_ORDER_CURVE]
# PARTITION_SIZES = [20000, 50000, 100000, 250000, 500000, 1000000]
PARTITION_SIZES = [50000]
NUM_COLUMNS = [2, 3, 4, 5, 6, 7]

DATASET_SCALED = False
DATA_FORMAT = '.parquet'
RESULTS_FOLDER = 'results'
RESULTS_FILE = os.path.join(RESULTS_FOLDER, 'results.csv')
RESULTS_LOG_FILE = 'results.log'
PARTITIONS_LOG_FILE = 'partitions.log'
ROW_GROUPS_LOG_FILE = 'row_groups.log'
ROWS_LOG_FILE = 'rows.log'

LOG_TO_CONSOLE = True
LOG_TO_FILE = True

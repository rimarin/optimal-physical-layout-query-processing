import os

TPCH_SF1 = 'tpch-sf1'
TPCH_SF10 = 'tpch-sf10'
OSM = 'osm'
TAXI = 'taxi'
DATASETS = [TPCH_SF1, TPCH_SF10, TAXI, OSM]

NO_PARTITION = 'no-partition'
FIXED_GRID = 'fixed-grid'
GRID_FILE = 'grid-file'
HILBERT_CURVE = 'hilbert-curve'
KD_TREE = 'kd-tree'
QUAD_TREE = 'quad-tree'
STR_TREE = 'str-tree'
Z_ORDER_CURVE = 'z-order-curve'
PARTITIONINGS = [FIXED_GRID, GRID_FILE, HILBERT_CURVE, KD_TREE, QUAD_TREE, STR_TREE, Z_ORDER_CURVE]
PARTITION_SIZES = [1000, 10000, 50000, 100000, 250000, 500000, 1000000]

DATA_FORMAT = '.parquet'
RESULTS_FOLDER = 'results'
RESULTS_FILE = os.path.join(RESULTS_FOLDER, 'results.csv')
RESULTS_LOG_FILE = 'results.log'
PARTITIONS_LOG_FILE = 'partitions.log'

LOG_TO_CONSOLE = True
LOG_TO_FILE = True

NUM_QUERIES = {
    TPCH_SF1: 7,
    TPCH_SF10: 7,
    TAXI: 56,
    OSM: 56
}

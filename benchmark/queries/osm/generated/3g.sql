-- Q3
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/osm/*.parquet') where min_lon > 42.25715599400593 AND min_lon < 175.94209145432558 AND
      max_lon > 30.390196290408852 AND max_lon < 93.36224801117658 AND
      min_lat > -58.992115099488075 AND min_lat < 49.44158682880942
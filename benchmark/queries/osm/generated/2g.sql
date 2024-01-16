-- Q2
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/osm/*.parquet') where min_lon > -107.20135277821102 AND min_lon < 169.04362418992548 AND
      max_lon > -67.12916957644686 AND max_lon < -45.11355919283545
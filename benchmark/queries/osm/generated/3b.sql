-- Q3
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/osm/*.parquet') where min_lon > -1.8942193842386814 AND min_lon < 58.66645329426038 AND
      max_lon > -149.23471187545044 AND max_lon < 124.71323971708398 AND
      min_lat > 16.45867200674391 AND min_lat < 18.06036459148008
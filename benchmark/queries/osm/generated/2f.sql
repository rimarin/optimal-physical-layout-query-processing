-- Q2
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/osm/*.parquet') where min_lon > 51.341547472326596 AND min_lon < 76.98596741925644 AND
      max_lon > -124.80866945284373 AND max_lon < 140.59795320552473
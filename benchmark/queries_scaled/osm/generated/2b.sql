-- Q2
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/osm/no-partition/*.parquet') where min_lon > -79.86382298957575 AND min_lon < 108.5931845747183 AND
      max_lon > -19.65329642111368 AND max_lon < -16.91684642897394
-- Q3
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/osm/no-partition/*.parquet') where min_lon > -109.67157230299092 AND min_lon < 51.047005050011535 AND
      max_lon > -26.47322131773339 AND max_lon < -17.702121578457792 AND
      min_lat > -65.97083796773566 AND min_lat < 29.900093238234035
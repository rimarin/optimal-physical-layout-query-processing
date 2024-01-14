-- Q2
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/osm/no-partition/*.parquet') where min_lon > 80.06328670996885 AND min_lon < 151.5256517145662 AND
      max_lon > -93.34598808352581 AND max_lon < 85.68580563673521
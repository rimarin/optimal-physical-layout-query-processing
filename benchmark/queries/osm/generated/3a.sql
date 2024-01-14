-- Q3
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/osm/no-partition/*.parquet') where min_lon > -151.5691821477057 AND min_lon < 13.994252503204024 AND
      max_lon > -176.90203293955074 AND max_lon < -75.95849495297674 AND
      min_lat > -76.54848865527595 AND min_lat < -25.40201433138519
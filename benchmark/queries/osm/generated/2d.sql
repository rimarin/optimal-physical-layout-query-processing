-- Q2
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/osm/*.parquet') where min_lon > -47.71757714968129 AND min_lon < -38.755561146898174 AND
      max_lon > -75.96393226603733 AND max_lon < 56.99282792477072
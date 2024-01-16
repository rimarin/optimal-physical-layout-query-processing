-- Q4
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/osm/*.parquet') where min_lon > -73.12548104782474 AND min_lon < 47.695021479702774 AND
      max_lon > -143.93261069233594 AND max_lon < 90.12236951210173 AND
      min_lat > -61.74563097477653 AND min_lat < -0.37645965633977596 AND
      max_lat > -28.674451722108095 AND max_lat < 26.46715909041332
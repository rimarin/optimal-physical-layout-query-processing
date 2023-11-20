-- Q3
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/osm/*.parquet') where min_lon > 32.29239001964535 AND min_lon < 89.18982235034548 AND
      max_lon > -104.57221753343525 AND max_lon < 177.51587942042295 AND
      min_lat > 23.759562166593852 AND min_lat < 56.260579133237314
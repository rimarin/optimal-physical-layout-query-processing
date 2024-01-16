-- Q2
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/osm/*.parquet') where min_lon > -171.40823565164646 AND min_lon < 34.190307256220564 AND
      max_lon > -67.74169531289492 AND max_lon < -67.27043473800153
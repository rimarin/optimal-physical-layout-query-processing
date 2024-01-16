-- Q2
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/osm/*.parquet') where min_lon > -42.40253897196382 AND min_lon < -38.48196691043961 AND
      max_lon > -150.24489467601282 AND max_lon < -15.830915807512582
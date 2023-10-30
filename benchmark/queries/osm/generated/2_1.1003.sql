-- Q2
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/osm/*.parquet') where min_lon > -75.76565303225497 AND min_lon < 153.1170176580983 AND
      max_lon > 79.0511992969266 AND max_lon < 92.995187433294
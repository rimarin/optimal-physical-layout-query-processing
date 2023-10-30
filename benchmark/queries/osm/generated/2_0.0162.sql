-- Q2
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/osm/*.parquet') where min_lon > -149.24348407125723 AND min_lon < 29.057017855536202 AND
      max_lon > -158.02063756110704 AND max_lon < -135.22351533063375
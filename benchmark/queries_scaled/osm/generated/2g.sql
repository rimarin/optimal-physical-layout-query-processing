-- Q2
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/osm/no-partition/*.parquet') where min_lon > -62.28609204538597 AND min_lon < 61.25379042904103 AND
      max_lon > -153.63662531823087 AND max_lon < -47.564334707919585
-- Q2
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/osm/no-partition/*.parquet') where min_lon > -90.69569007743719 AND min_lon < -12.34802346301484 AND
      max_lon > -41.52223631195176 AND max_lon < 135.15340799370688
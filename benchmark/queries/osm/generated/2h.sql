-- Q2
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/osm/no-partition/*.parquet') where min_lon > 42.06626871359185 AND min_lon < 93.94798475001 AND
      max_lon > -65.54725062813161 AND max_lon < 152.80647996974096
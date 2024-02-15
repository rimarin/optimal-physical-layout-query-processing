-- Q6
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/osm/no-partition/*.parquet') where min_lon > -38.485880259851484 AND min_lon < 103.63446342528789 AND
      max_lon > -166.76253100525085 AND max_lon < -4.8648098083371565 AND
      min_lat > -62.6263776426417 AND min_lat < 73.11141482220921 AND
      max_lat > -78.08111681713116 AND max_lat < 4.503925385698949 AND
      created_at > '2008090609:31:21' AND created_at < '2022061414:26:08' AND
      version > 2.9272044405181297 AND version < 5.3992327614643045
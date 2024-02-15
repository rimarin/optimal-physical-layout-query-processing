-- Q6
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/osm/no-partition/*.parquet') where min_lon > 19.593445974407217 AND min_lon < 149.53380842019305 AND
      max_lon > 16.002833365970474 AND max_lon < 90.09843401016275 AND
      min_lat > -76.90519827358415 AND min_lat < 22.495190417183153 AND
      max_lat > -65.6315301632161 AND max_lat < 21.85387989137 AND
      created_at > '2006040721:45:52' AND created_at < '2016081303:15:54' AND
      version > 5.017280823144164 AND version < 9.027480972142476
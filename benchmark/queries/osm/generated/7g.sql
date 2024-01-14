-- Q7
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/osm/no-partition/*.parquet') where min_lon > -31.700039627296974 AND min_lon < 169.19212636685882 AND
      max_lon > -156.25184058100587 AND max_lon < 92.71653179312153 AND
      min_lat > 0.09860689061774508 AND min_lat < 17.463765330266483 AND
      max_lat > -54.207267093294625 AND max_lat < 35.579051290396976 AND
      created_at > '2008092820:58:19' AND created_at < '2023082522:44:44' AND
      version > 1.0366267179278141 AND version < 8.903252471438634 AND
      id > 3273447942.0360584 AND id < 6320583429.563833

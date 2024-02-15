-- Q6
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/osm/no-partition/*.parquet') where min_lon > 95.78845728723644 AND min_lon < 143.72315554218596 AND
      max_lon > 41.85148893703081 AND max_lon < 142.79589732299655 AND
      min_lat > -32.87545301552392 AND min_lat < 19.042923899526258 AND
      max_lat > -58.5198220235855 AND max_lat < 71.91722636387527 AND
      created_at > '2015041119:45:36' AND created_at < '2017032813:23:57' AND
      version > 1.8066541379386556 AND version < 8.204393268925195
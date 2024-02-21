-- Q7
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/osm/no-partition/*.parquet') where min_lon > -146.4946802675477 AND min_lon < 59.94538590484652 AND
      max_lon > 12.105711217940353 AND max_lon < 158.35831887176477 AND
      min_lat > -73.74314983517988 AND min_lat < 84.8475528840973 AND
      max_lat > 2.2381563317031947 AND max_lat < 77.9167408442676 AND
      created_at > '2006020305:43:04' AND created_at < '2014042917:20:26' AND
      version > 2 AND version < 4 AND
      id > 2765133570 AND id < 4464545717

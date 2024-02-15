-- Q6
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/osm/no-partition/*.parquet') where min_lon > -87.43356977964068 AND min_lon < 85.85921084617473 AND
      max_lon > -18.616964490214116 AND max_lon < 178.9635216419927 AND
      min_lat > -7.283246726989063 AND min_lat < -5.883986662863876 AND
      max_lat > -27.09883524722241 AND max_lat < 66.48331979231031 AND
      created_at > '2011092815:58:31' AND created_at < '2018020104:50:42' AND
      version > 2.061609185338243 AND version < 8.868848080529006
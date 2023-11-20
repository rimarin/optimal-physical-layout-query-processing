-- Q5
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/osm/*.parquet') where min_lon > -46.174726321237586 AND min_lon < -19.137843332874525 AND
      max_lon > -172.87389661072413 AND max_lon < -42.50952928029449 AND
      min_lat > -88.12196693031588 AND min_lat < 82.59537152329438 AND
      max_lat > -34.70481915630642 AND max_lat < 72.34828018746109 AND
      created_at > '2011030119:13:04' AND created_at < '2012100710:28:49'
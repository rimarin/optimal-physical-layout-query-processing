-- Q5
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/osm/*.parquet') where min_lon > -72.27427604159145 AND min_lon < -0.3435994927124284 AND
      max_lon > -130.44308034852276 AND max_lon < 168.46588395449203 AND
      min_lat > 20.447917345631637 AND min_lat < 87.86487843255432 AND
      max_lat > -43.77039600423282 AND max_lat < 50.87789047156099 AND
      created_at > '2012030401:33:31' AND created_at < '2016032620:04:50'
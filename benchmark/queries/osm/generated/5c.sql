-- Q5
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/osm/*.parquet') where min_lon > 22.825178918805193 AND min_lon < 94.12500123741631 AND
      max_lon > -87.00526409852101 AND max_lon < 54.76319311384108 AND
      min_lat > -1.1551609363112902 AND min_lat < 85.29034391833801 AND
      max_lat > -70.47905606489033 AND max_lat < 40.70662089467544 AND
      created_at > '2006052311:05:31' AND created_at < '2008093002:39:40'
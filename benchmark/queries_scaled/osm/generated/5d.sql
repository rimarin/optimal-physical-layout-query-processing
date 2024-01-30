-- Q5
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/osm/*.parquet') where min_lon > -170.63147213283497 AND min_lon < 54.06874038840303 AND
      max_lon > -123.37949753615116 AND max_lon < 76.73661009736287 AND
      min_lat > -88.08605437938981 AND min_lat < -0.536969314674522 AND
      max_lat > -21.25462241607221 AND max_lat < -7.799475564789645 AND
      created_at > '2009120403:37:11' AND created_at < '2014062015:38:36'
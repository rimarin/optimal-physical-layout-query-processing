-- Q5
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/osm/*.parquet') where min_lon > 8.469940446690373 AND min_lon < 122.68819913256925 AND
      max_lon > -29.60013303727891 AND max_lon < 35.29112636432913 AND
      min_lat > 16.18026871293442 AND min_lat < 23.587876263984285 AND
      max_lat > -21.117303380489986 AND max_lat < 38.42873212655914 AND
      created_at > '2007010805:53:28' AND created_at < '2019120522:05:39'
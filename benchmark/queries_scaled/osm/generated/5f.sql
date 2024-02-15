-- Q5
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/osm/no-partition/*.parquet') where min_lon > -30.495755961002544 AND min_lon < 94.81159403647712 AND
      max_lon > 5.296332527329383 AND max_lon < 48.968306650867305 AND
      min_lat > -83.62430092017917 AND min_lat < 30.335911474542712 AND
      max_lat > 14.692041863779949 AND max_lat < 57.8027792446388 AND
      created_at > '2019122401:04:35' AND created_at < '2022120802:20:08'
-- Q4
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/osm/no-partition/*.parquet') where min_lon > -149.6023395718592 AND min_lon < -93.50894473018468 AND
      max_lon > -151.90723739499288 AND max_lon < 51.170260304483435 AND
      min_lat > -29.685590004803984 AND min_lat < 19.624403814063996 AND
      max_lat > 9.075491826452108 AND max_lat < 62.36567609424901
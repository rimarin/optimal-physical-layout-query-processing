-- Q5
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/osm/no-partition/*.parquet') where min_lon > -39.94201360969993 AND min_lon < -24.662157244470393 AND
      max_lon > -79.35426989900994 AND max_lon < 161.92400969866736 AND
      min_lat > -65.70831768665784 AND min_lat < -5.036833037673475 AND
      max_lat > -28.537269572800042 AND max_lat < 9.33677640997206 AND
      created_at > '2013040608:58:17' AND created_at < '2022061916:15:04'
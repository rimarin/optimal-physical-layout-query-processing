-- Q5
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/osm/no-partition/*.parquet') where min_lon > -125.86675107193324 AND min_lon < 151.26475319511474 AND
      max_lon > -25.13926037204223 AND max_lon < -4.274719915677082 AND
      min_lat > -52.36775638898363 AND min_lat < 40.497930059023446 AND
      max_lat > 3.091455106765693 AND max_lat < 14.408366842248668 AND
      created_at > '2007121300:47:04' AND created_at < '2016071621:35:39'
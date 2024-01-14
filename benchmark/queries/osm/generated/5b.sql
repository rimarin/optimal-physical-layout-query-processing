-- Q5
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/osm/no-partition/*.parquet') where min_lon > -110.29024571174737 AND min_lon < 129.9172297359782 AND
      max_lon > -158.30163553986517 AND max_lon < 166.85300228379873 AND
      min_lat > -31.470823647285805 AND min_lat < -3.8198535757522336 AND
      max_lat > -8.884778826341034 AND max_lat < 37.87773504820288 AND
      created_at > '2007021904:13:21' AND created_at < '2009112605:39:20'
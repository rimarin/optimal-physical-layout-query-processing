-- Q6
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/osm/no-partition/*.parquet') where min_lon > -109.73493277174246 AND min_lon < -9.559115051385817 AND
      max_lon > -101.38038250477547 AND max_lon < -54.30370166690501 AND
      min_lat > 28.98472510763679 AND min_lat < 62.33426545881673 AND
      max_lat > -67.10645716806359 AND max_lat < 79.84181037660153 AND
      created_at > '2011061901:40:41' AND created_at < '2013011700:19:33' AND
      version > 1 AND version < 8
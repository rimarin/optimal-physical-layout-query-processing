-- Q6
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/osm/*.parquet') where min_lon > -51.978313144628856 AND min_lon < 173.38163973557386 AND
      max_lon > -102.4967659789697 AND max_lon < 102.6342505575974 AND
      min_lat > -78.38413722488362 AND min_lat < 68.43663255678919 AND
      max_lat > -36.773336093173874 AND max_lat < 23.736267921486515 AND
      created_at > '2006071208:36:06' AND created_at < '2012071409:12:20' AND
      version > 1.9696200072408652 AND version < 3.703297082420722
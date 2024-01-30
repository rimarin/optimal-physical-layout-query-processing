-- Q4
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/osm/*.parquet') where min_lon > -111.16814433136297 AND min_lon < 70.62917294200895 AND
      max_lon > -90.39332512927048 AND max_lon < 100.88357016765934 AND
      min_lat > -70.13215761850672 AND min_lat < 30.66993962732724 AND
      max_lat > -59.62766757723118 AND max_lat < -13.40395249305584
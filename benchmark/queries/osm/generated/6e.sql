-- Q6
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/osm/*.parquet') where max_lon > -90.39332512927048 AND max_lon < 100.88357016765934 AND
      min_lat > -70.13215761850672 AND min_lat < 30.66993962732724 AND
      max_lat > -59.62766757723118 AND max_lat < -13.40395249305584 AND
      created_at > '2012071903:10:20' AND created_at < '2020122400:24:50' AND
      version > 6.132811480789868 AND version < 287.108527204043
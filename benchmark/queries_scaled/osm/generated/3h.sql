-- Q3
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/osm/no-partition/*.parquet') where min_lon > -83.22870402687356 AND min_lon < 134.83206118654323 AND
      max_lon > 115.05424371077083 AND max_lon < 158.69738377143693 AND
      min_lat > -20.816884480018288 AND min_lat < 57.11785042021404
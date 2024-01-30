-- Q1
SELECT *
FROM read_parquet('datasets/osm/*.parquet')
WHERE min_lon > ':1' AND min_lon < ':2'

-- Q3
SELECT *
FROM read_parquet('datasets/osm/*.parquet')
WHERE min_lon > ':1' AND min_lon < ':2' AND
      max_lon > ':3' AND max_lon < ':4' AND
      min_lat > ':5' AND min_lat < ':6'
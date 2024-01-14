-- Q6
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/osm/no-partition/*.parquet') where min_lon > -169.05124477974366 AND min_lon < 170.3828264217439 AND
      max_lon > -134.44864835181374 AND max_lon < 129.07443063037414 AND
      min_lat > -64.22437746299698 AND min_lat < 33.31194233272056 AND
      max_lat > -71.46936157823774 AND max_lat < -15.200168409242067 AND
      created_at > '2007030813:55:02' AND created_at < '2019021501:00:55' AND
      version > 8.15904227286897 AND version < 9.324925758163152
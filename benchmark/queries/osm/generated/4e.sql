-- Q4
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/osm/*.parquet') where min_lon > -94.5226755932016 AND min_lon < 75.54333712477927 AND
      max_lon > -43.43546712437805 AND max_lon < 93.6291777317706 AND
      min_lat > -88.15026170870524 AND min_lat < -0.41918465630355684 AND
      max_lat > -12.968197584104644 AND max_lat < 52.04196597282751
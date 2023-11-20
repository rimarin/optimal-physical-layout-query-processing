-- Q4
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/osm/*.parquet') where min_lon > -137.67577448909927 AND min_lon < -45.45217361867225 AND
      max_lon > -60.890877753608734 AND max_lon < -29.48783674231356 AND
      min_lat > -75.34542219064562 AND min_lat < 44.90399073450007 AND
      max_lat > -21.463631378500565 AND max_lat < -1.0356077006452153
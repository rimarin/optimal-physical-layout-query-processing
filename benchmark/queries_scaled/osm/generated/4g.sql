-- Q4
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/osm/no-partition/*.parquet') where min_lon > -126.27824088498014 AND min_lon < 20.294599885306326 AND
      max_lon > -94.59134781260893 AND max_lon < 74.15683821892196 AND
      min_lat > 17.567928382510758 AND min_lat < 66.00460058409314 AND
      max_lat > 57.620154637041736 AND max_lat < 70.34615787486089
-- Q4
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/osm/no-partition/*.parquet') where min_lon > -173.12871473881984 AND min_lon < 147.60007129661676 AND
      max_lon > -142.24991835638136 AND max_lon < 128.2294531679421 AND
      min_lat > -60.02864316831897 AND min_lat < 49.63288796716162 AND
      max_lat > -78.49186555405942 AND max_lat < -43.19169548075011
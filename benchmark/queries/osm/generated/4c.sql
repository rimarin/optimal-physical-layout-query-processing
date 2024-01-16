-- Q4
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/osm/*.parquet') where min_lon > -118.9888217833567 AND min_lon < -99.14582527419262 AND
      max_lon > -119.12696868168071 AND max_lon < 2.1114918800962243 AND
      min_lat > -13.431373970672425 AND min_lat < 20.32782837246242 AND
      max_lat > -1.270476897341723 AND max_lat < 88.03023062037687
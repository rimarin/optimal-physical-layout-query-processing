-- Q4
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/osm/no-partition/*.parquet') where min_lon > -152.48412867840108 AND min_lon < 11.722905382182148 AND
      max_lon > -124.03708833421527 AND max_lon < -10.496398406017363 AND
      min_lat > -78.12494928424772 AND min_lat < 4.11449682520805 AND
      max_lat > -82.20468388327052 AND max_lat < 1.5969040488855342
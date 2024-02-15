-- Q4
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/osm/no-partition/*.parquet') where min_lon > -155.32803145970502 AND min_lon < -116.94177609164035 AND
      max_lon > -169.267703293425 AND max_lon < -87.7076163453373 AND
      min_lat > 13.724850794040961 AND min_lat < 34.96595458246895 AND
      max_lat > 24.411666881438578 AND max_lat < 34.493370213302285
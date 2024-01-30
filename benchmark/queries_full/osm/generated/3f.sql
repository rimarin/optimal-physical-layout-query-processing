-- Q3
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/osm/*.parquet') where min_lon > 19.829535568398796 AND min_lon < 158.66826705695894 AND
      max_lon > -10.890239094989937 AND max_lon < 171.70482248153934 AND
      min_lat > 42.43985548682184 AND min_lat < 47.996938518273026
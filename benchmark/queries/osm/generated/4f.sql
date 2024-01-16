-- Q4
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/osm/*.parquet') where min_lon > -140.67167814112082 AND min_lon < 14.573058451299573 AND
      max_lon > -138.17220342175716 AND max_lon < 79.10245330629107 AND
      min_lat > -37.81171770228235 AND min_lat < 2.9377256900321385 AND
      max_lat > -26.707386757145173 AND max_lat < 64.6859568589474
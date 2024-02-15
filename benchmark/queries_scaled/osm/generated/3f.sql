-- Q3
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/osm/no-partition/*.parquet') where min_lon > -168.5516789319937 AND min_lon < -105.803480957448 AND
      max_lon > -155.69595947753243 AND max_lon < 69.12614331471252 AND
      min_lat > 20.5123115194734 AND min_lat < 34.00066407602171
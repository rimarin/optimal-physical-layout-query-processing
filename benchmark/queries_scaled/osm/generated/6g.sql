-- Q6
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/osm/no-partition/*.parquet') where min_lon > -157.67714834466273 AND min_lon < 0.32230190089663324 AND
      max_lon > -145.0434544887785 AND max_lon < -76.44928368676926 AND
      min_lat > -79.00417390920205 AND min_lat < 25.565827946026076 AND
      max_lat > -85.71953828167086 AND max_lat < 8.469390529393166 AND
      created_at > '2016112223:44:39' AND created_at < '2019102519:10:38' AND
      version > 1 AND version < 6
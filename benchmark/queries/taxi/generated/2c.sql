-- Q2
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/taxi/no-partition/*.parquet') where PULocationID > 67.7736515279541 AND PULocationID < 92.82960812284081 AND
      DOLocationID > 33.423585434543824 AND DOLocationID < 42.72493214251795

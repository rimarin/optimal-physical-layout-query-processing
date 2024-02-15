-- Q2
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/taxi/no-partition/*.parquet') where PULocationID > 191.16795466918475 AND PULocationID < 201.58744241898287 AND
      DOLocationID > 13.894469678887239 AND DOLocationID < 205.56397279099883

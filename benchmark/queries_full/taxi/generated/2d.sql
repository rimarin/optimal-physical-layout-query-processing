-- Q2
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/taxi/*.parquet') where PULocationID > 9.695796833384954 AND PULocationID < 167.37867910893266 AND
      DOLocationID > 95.97171597256164 AND DOLocationID < 99.61036342789092

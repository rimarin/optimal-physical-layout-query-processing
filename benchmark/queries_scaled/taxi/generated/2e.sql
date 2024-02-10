-- Q2
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/taxi/*.parquet') where PULocationID > 42.221315746960535 AND PULocationID < 68.92041997339238 AND
      DOLocationID > 19.51834457906559 AND DOLocationID < 50.687216857913164

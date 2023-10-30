-- Q2
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/taxi/trips*.parquet') where PULocationID > 162.46387492212264 AND PULocationID < 250.51785206473073 AND
      DOLocationID > 96.64439744418506 AND DOLocationID < 129.278918193403

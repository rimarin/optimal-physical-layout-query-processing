-- Q1
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/taxi/no-partition/*.parquet') where PULocationID > 69.43561442589343 AND PULocationID < 77.91779514624572

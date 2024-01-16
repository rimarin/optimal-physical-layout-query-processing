-- Q2
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/taxi/trips*.parquet') where PULocationID > 88.27840305409433 AND PULocationID < 164.04182478321383 AND
      DOLocationID > 162.9489252314513 AND DOLocationID < 165.41289906721408

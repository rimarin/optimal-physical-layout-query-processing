-- Q2
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/taxi/no-partition/*.parquet') where PULocationID > 129.52788054320652 AND PULocationID < 193.36995644581523 AND
      DOLocationID > 46.2786959342697 AND DOLocationID < 79.89332387836889

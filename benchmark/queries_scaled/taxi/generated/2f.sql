-- Q2
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/taxi/no-partition/*.parquet') where PULocationID > 3.4855874969717666 AND PULocationID < 19.44927443329663 AND
      DOLocationID > 18.819280049586222 AND DOLocationID < 232.012834798668

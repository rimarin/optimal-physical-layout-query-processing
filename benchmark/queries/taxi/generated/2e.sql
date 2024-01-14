-- Q2
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/taxi/no-partition/*.parquet') where PULocationID > 121.06996293416644 AND PULocationID < 149.97992640079144 AND
      DOLocationID > 184.20855477705666 AND DOLocationID < 223.83570359340456

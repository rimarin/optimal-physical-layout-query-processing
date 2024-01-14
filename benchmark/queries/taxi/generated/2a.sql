-- Q2
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/taxi/no-partition/*.parquet') where PULocationID > 175.64729003884784 AND PULocationID < 181.17527569907827 AND
      DOLocationID > 76.74741907618467 AND DOLocationID < 81.36052943601304

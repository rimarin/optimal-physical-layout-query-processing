-- Q2
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/taxi/*.parquet') where PULocationID > 159.06584041234316 AND PULocationID < 241.58929586952743 AND
      DOLocationID > 46.85978942209633 AND DOLocationID < 47.512436191690725

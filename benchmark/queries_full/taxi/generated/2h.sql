-- Q2
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/taxi/*.parquet') where PULocationID > 30.637830184018846 AND PULocationID < 135.0799137551255 AND
      DOLocationID > 43.163012782109035 AND DOLocationID < 81.49920328976529

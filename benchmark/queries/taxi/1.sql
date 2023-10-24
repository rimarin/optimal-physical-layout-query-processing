-- Q1
SELECT COUNT(*)
FROM read_parquet('datasets/taxi/*.parquet')
WHERE PULocationID > ':1' AND PULocationID < ':2'

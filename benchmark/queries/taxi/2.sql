-- Q2
SELECT COUNT(*)
FROM read_parquet('datasets/taxi/*.parquet')
WHERE PULocationID > ':1' AND PULocationID < ':2'
      DOLocationID > ':3' AND DOLocationID < ':4'


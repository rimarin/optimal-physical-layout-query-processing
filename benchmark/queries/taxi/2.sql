-- Q2
SELECT *
FROM read_parquet('datasets/taxi/*.parquet')
WHERE PULocationID > ':1' AND PULocationID < ':2' AND
      DOLocationID > ':3' AND DOLocationID < ':4'

-- Q6
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/taxi/*.parquet') where PULocationID > 37.86685561124625 AND PULocationID < 257.5177434217682 AND
      DOLocationID > 147.6029795592706 AND DOLocationID < 186.87292787219675
AND tpep_pickup_datetime > '2018032914:40:43' AND
    tpep_pickup_datetime < '2019090321:45:52' AND
    tpep_dropoff_datetime > '2018071309:17:33' AND
    tpep_dropoff_datetime < '2019052323:23:20'
AND
    passenger_count > 1.3972071976544438 AND passenger_count < 3.1039269388311173 AND
    fare_amount > 8.510967760635147 AND fare_amount < 370.1165562417815

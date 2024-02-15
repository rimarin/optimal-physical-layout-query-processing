-- Q7
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/osm/no-partition/*.parquet') where min_lon > -132.76378619101877 AND min_lon < 172.49717284067577 AND
      max_lon > -77.21559553475461 AND max_lon < 171.2616251009809 AND
      min_lat > -50.609930013940854 AND min_lat < 76.35050780596046 AND
      max_lat > -87.79787708326675 AND max_lat < 55.91636789936979 AND
      created_at > '2008090705:36:04' AND created_at < '2020022803:05:28' AND
      version > 4.688017124136953 AND version < 5.9975317460655715 AND
      id > 2912610180.398409 AND id < 3188578806.1298695

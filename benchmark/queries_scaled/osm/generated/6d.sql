-- Q6
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/osm/no-partition/*.parquet') where min_lon > -176.68858864193012 AND min_lon < 45.212572438387355 AND
      max_lon > -140.4913231444094 AND max_lon < -16.875975361257446 AND
      min_lat > -77.50711636388648 AND min_lat < 64.4745212958679 AND
      max_lat > -66.00317125691431 AND max_lat < -22.87993167866209 AND
      created_at > '2007050514:48:08' AND created_at < '2017031020:11:35' AND
      version > 4 AND version < 7
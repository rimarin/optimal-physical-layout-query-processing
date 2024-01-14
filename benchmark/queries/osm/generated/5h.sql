-- Q5
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/osm/no-partition/*.parquet') where min_lon > -81.04387131474635 AND min_lon < -16.870142502155915 AND
      max_lon > -142.87546758379847 AND max_lon < 78.5985970280077 AND
      min_lat > -50.0308295859397 AND min_lat < 15.845868611575256 AND
      max_lat > -69.98679582414928 AND max_lat < 17.045952890284966 AND
      created_at > '2010020514:16:06' AND created_at < '2021083019:13:51'
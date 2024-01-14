-- Q7
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/osm/no-partition/*.parquet') where min_lon > -99.75581760626129 AND min_lon < -29.93999794060352 AND
      max_lon > -110.4396767152472 AND max_lon < -5.308694870299149 AND
      min_lat > -84.67817311054327 AND min_lat < 18.335453888384194 AND
      max_lat > -39.945009423314886 AND max_lat < 64.70980052330168 AND
      created_at > '2021113018:12:03' AND created_at < '2022122121:18:51' AND
      version > 3.2146049183207634 AND version < 9.869395167106623 AND
      id > 3093096877.698008 AND id < 9766716863.675241

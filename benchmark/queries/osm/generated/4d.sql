-- Q4
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/osm/no-partition/*.parquet') where min_lon > 38.421835838295124 AND min_lon < 101.85232818766201 AND
      max_lon > -61.21368636834262 AND max_lon < 123.65563368557497 AND
      min_lat > -57.59097009819506 AND min_lat < 12.204187684923241 AND
      max_lat > -61.525914913269986 AND max_lat < -4.273623293401528
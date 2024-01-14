-- Q7
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/osm/no-partition/*.parquet') where min_lon > -177.18431517696766 AND min_lon < -2.376201444686501 AND
      max_lon > -27.591856270361944 AND max_lon < -20.873878368189168 AND
      min_lat > -30.106690992726783 AND min_lat < 82.37106413515937 AND
      max_lat > -84.75854088194654 AND max_lat < 65.30910666860868 AND
      created_at > '2008052005:45:33' AND created_at < '2022101514:28:48' AND
      version > 1.3462931060717254 AND version < 8.95750001302418 AND
      id > 1249050995.029004 AND id < 10585613190.353653

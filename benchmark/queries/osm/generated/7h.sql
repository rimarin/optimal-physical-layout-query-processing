-- Q7
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/osm/no-partition/*.parquet') where min_lon > -151.03976177695785 AND min_lon < -23.07970113812931 AND
      max_lon > -136.46830985658247 AND max_lon < 4.870809990102231 AND
      min_lat > -36.25708546901055 AND min_lat < 12.164549028451972 AND
      max_lat > -57.22752916894189 AND max_lat < 49.899202672786146 AND
      created_at > '2007111209:03:36' AND created_at < '2015062823:06:30' AND
      version > 1.9802257024704355 AND version < 7.72357301464376 AND
      id > 551241424.8103608 AND id < 3184262153.663622

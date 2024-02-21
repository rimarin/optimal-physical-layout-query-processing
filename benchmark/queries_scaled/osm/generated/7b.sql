-- Q7
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/osm/no-partition/*.parquet') where min_lon > -170.29200534776294 AND min_lon < 109.73950510658972 AND
      max_lon > 41.262468460640264 AND max_lon < 103.37513973254448 AND
      min_lat > -15.943477323683382 AND min_lat < 37.71832329495484 AND
      max_lat > 19.824204594167142 AND max_lat < 63.207703022535156 AND
      created_at > '2006090116:07:51' AND created_at < '2011112505:26:45' AND
      version > 1 AND version < 3 AND
      id > 631633568 AND id < 6504490594

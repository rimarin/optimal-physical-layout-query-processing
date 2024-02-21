-- Q7
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/osm/*.parquet') where min_lon > 9.497337245576176 AND min_lon < 95.71801842452254 AND
      max_lon > -104.7220049013859 AND max_lon < 170.43951041513498 AND
      min_lat > -82.3884030062193 AND min_lat < 73.93095975785843 AND
      max_lat > -17.35499057103864 AND max_lat < 27.28220371003445 AND
      created_at > '2008061413:14:33' AND created_at < '2016081308:40:28' AND
      version > 1 AND version < 3 AND
      id > 2895924731 AND id < 8781764799


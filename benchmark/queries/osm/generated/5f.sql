-- Q5
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/osm/*.parquet') where min_lon > -117.12933767946615 AND min_lon < 171.54038064844076 AND
      max_lon > 60.67742767646516 AND max_lon < 163.81888086571337 AND
      min_lat > -23.032986237930572 AND min_lat < 57.72021773470024 AND
      max_lat > -45.73485865671075 AND max_lat < 64.4007855456434 AND
      created_at > '2017081809:53:12' AND created_at < '2018060117:31:32'
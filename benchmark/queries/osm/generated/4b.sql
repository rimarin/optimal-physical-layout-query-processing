-- Q4
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/osm/*.parquet') where min_lon > 10.36458012399035 AND min_lon < 129.67514361051605 AND
      max_lon > 64.82888990558743 AND max_lon < 152.54214993985437 AND
      min_lat > -88.16336685190227 AND min_lat < 13.33209604037772 AND
      max_lat > -34.338464849548046 AND max_lat < -31.828620474452137
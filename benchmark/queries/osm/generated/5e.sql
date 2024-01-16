-- Q5
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/osm/*.parquet') where min_lon > 21.262639869867087 AND min_lon < 40.76962623805227 AND
      max_lon > -128.0460899965776 AND max_lon < 96.57090148083421 AND
      min_lat > -44.025407798767404 AND min_lat < -25.545988630791143 AND
      max_lat > -64.57768997188771 AND max_lat < 55.09439595900284 AND
      created_at > '2016122923:49:40' AND created_at < '2022042410:26:49'
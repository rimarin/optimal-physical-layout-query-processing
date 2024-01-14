-- Q5
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/osm/no-partition/*.parquet') where min_lon > -114.59656676175507 AND min_lon < 20.8291451758148 AND
      max_lon > -25.791895899573746 AND max_lon < 81.80819190141028 AND
      min_lat > -30.956890939978614 AND min_lat < -27.678763383779398 AND
      max_lat > -80.73154105998869 AND max_lat < -20.826915668964773 AND
      created_at > '2014030806:34:32' AND created_at < '2021081400:08:33'
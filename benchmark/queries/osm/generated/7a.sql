-- Q7
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/osm/no-partition/*.parquet') where min_lon > -14.749383579159087 AND min_lon < 162.91953627724814 AND
      max_lon > -100.07812773347895 AND max_lon < 168.648175773434 AND
      min_lat > -55.181147188991346 AND min_lat < -13.212002499500528 AND
      max_lat > -51.89304121549421 AND max_lat < 33.539667193668464 AND
      created_at > '2010122715:52:39' AND created_at < '2021081214:33:25' AND
      version > 6.917501275673032 AND version < 7.132274726976854 AND
      id > 3454608711.1797795 AND id < 10214965712.152706

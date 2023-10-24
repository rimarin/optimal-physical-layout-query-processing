select
     l_orderkey,
     sum(l_extendedprice * (1 - l_discount)) as revenue,
     o_orderdate,
     o_shippriority
FROM read_parquet('datasets/tpch/*.parquet') where
         c_mktsegment = 'BUILDING'
         and c_custkey = o_custkey
         and l_orderkey = o_orderkey
         and o_orderdate < date '1997-01-01'
         and l_shipdate > date '1997-04-24'
group by
     l_orderkey,
     o_orderdate,
     o_shippriority
order by
     revenue desc,
     o_orderdate;

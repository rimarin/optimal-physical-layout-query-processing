# Towards an Optimal Physical Layout for Efficient Query Processing in the Cloud

### Motivation
With the advent of cloud-based data warehouses, a new paradigm that separates compute and storage has been established.
In this context, the use of indexing is less common, in favour of the use of partitioning. This trend is justified by the 
impractical growth of the index size. Moreover, partitioning based on storage formats that keep statistics about the data
(e.g. Parquet) can intersect this information with the query predicates and retrieve only the necessary partitions to 
answer the query. Pruning the data blocks leads to faster query processing and reduced network costs. 

Recently, more techniques have been introduced to support compression and clustering of data, which facilitate efficient data access. 
The combination of several factors – such as the query selectivity, the presence of dimension ordering – and the choice of the physical
layout highly affects the performance of the query. For example, ordering on one dimension does not benefit queries that select other 
unordered columns. Data structures such as R-tree are limited by the curse of dimensionality and show degraded performance as the number
of dimensions increases. By measuring these conditions, it is possible to move towards an approach that efficiently performs range
selection of data, while maintaining stable performance for varying queries.
The goal of this project is to quantify how these multidimensional partitioning techniques perform along different datasets,
partition size and workloads.

### Project
The implementation includes:
- 7 multidimensional structures – Fixed-Grid, Grid-file, kd-tree, Quadtree, STRTree, Z-curve ordering, Hilbert curve 
- read, write and partitioning of data based on such structures.

Furthermore, for the benchmarking part:
- Design of a set of queries to use in the measurements. 
- Benchmarks computing the numbers of partitions retrieved by the queries across different structures, queries and partition size

### Installation

#### Partitioner

From the main 

    cmake .
    cmake --build .

### Related Work

Learned indexes: [Flood](https://dl.acm.org/doi/10.1145/3318464.3380579) [Tsunami](https://dl.acm.org/doi/10.14778/3425879.3425880)
Storage format: ...
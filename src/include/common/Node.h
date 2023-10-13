#ifndef PARTITIONING_NODE_H
#define PARTITIONING_NODE_H


namespace partitioning {

    class Node {
    public:
        KDTree(std::vector<std::string> partitionColumns);
        virtual ~KDTree() = default;
        arrow::Result<std::vector<std::shared_ptr<arrow::Table>>> partition(std::shared_ptr<arrow::Table> table);
    private:
        std::vector<std::string> columns;
    };
}

#endif //PARTITIONING_KDTREE_H

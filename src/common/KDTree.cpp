#include "include/common/KDTree.h"

namespace partitioning {

KDTree::KDTree(std::vector<arrow::Array> pointList) {
    pointList = std::move(pointList);
}

KDTree buildTreeRecursive(std::vector<arrow::Array> values, int n_dimensions){
    if (values.size() <= 1) return nullptr;

    // Get axis to split along
    int axis = depth % dim;

    int** sorted = sortAlongDim (values, axis);

    int mid = len / 2;
    Node* curr = new Node ();
    curr.point[0] = sorted [0][mid];
    curr.point[1] = sorted [1][mid];

    int** leftHalf = values;
    int** rightHalf = &(values [mid]);

    curr.left = createtreeRecursive (leftHalf, mid, dim, depth + 1);
    curr.right = createtreeRecursive (rightHalf, len - mid, dim, depth + 1);

    return curr;
}

std::vector<struct KDNode> getLeaves(){

}

}
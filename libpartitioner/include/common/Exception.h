#ifndef COMMON_EXCEPTION_H
#define COMMON_EXCEPTION_H

#include <iostream>

class InsufficientNumberOfColumns : public std::runtime_error
{
public:
    explicit InsufficientNumberOfColumns(const size_t minNumberOfColumns) :
        runtime_error("Required at least " + std::to_string(minNumberOfColumns) + "columns") {}
};

class InvalidPartitionSize : public std::runtime_error
{
public:
    InvalidPartitionSize(const size_t minPartitionSize, const size_t maxPartitionSize) :
        runtime_error("Invalid partition size, valid value between " + std::to_string(minPartitionSize) +
                      " and " + std::to_string(maxPartitionSize)) {};
};

class InvalidColumn : public std::runtime_error
{
public:
    explicit InvalidColumn(const std::string& columnName) :
        runtime_error("Invalid column " + columnName + " not found in the schema") {};
};


#endif //COMMON_EXCEPTION_H



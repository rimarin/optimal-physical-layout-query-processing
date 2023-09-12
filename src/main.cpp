#include <arrow/io/api.h>
#include <iostream>

#include "storage/DataWriter.h"
#include "storage/DataWriter.cpp"


int main() {
    arrow::Status st = oplqp::DataWriter().GenInitialFile();
    if (!st.ok()) {
        std::cerr << st << std::endl;
        return 1;
    }
    return 0;
}

#ifndef OPTIMAL_PHYSICAL_LAYOUT_QUERY_PROCESSING_DATAWRITER_H
#define OPTIMAL_PHYSICAL_LAYOUT_QUERY_PROCESSING_DATAWRITER_H

#include <arrow/api.h>
#include <arrow/csv/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/api/reader.h>
#include <parquet/api/writer.h>

namespace oplqp {

    class DataWriter {
    public: DataWriter();
    public: static arrow::Status GenInitialFile();

    };
} // oplqp

#endif //OPTIMAL_PHYSICAL_LAYOUT_QUERY_PROCESSING_DATAWRITER_H

#include <set>
#include <string>

#include "storage/DataReader.h"
#include "partitioning/FixedGridPartitioning.h"
#include "partitioning/GridFilePartitioning.h"
#include "partitioning/HilbertCurvePartitioning.h"
#include "partitioning/KDTreePartitioning.h"
#include "partitioning/NoPartitioning.h"
#include "partitioning/QuadTreePartitioning.h"
#include "partitioning/STRTreePartitioning.h"
#include "partitioning/ZOrderCurvePartitioning.h"


class ExperimentsConfig {
public:

    // Dataset names
    static inline const std::string datasetSchool = "school";
    static inline const std::string datasetWeather = "weather";
    static inline const std::string datasetCities = "cities";
    static inline const std::set<std::string> testDatasets = {datasetSchool, datasetWeather, datasetCities};
    static inline const std::string datasetOSM = "osm";
    static inline const std::string datasetTaxi = "taxi";
    static inline const std::string datasetTPCH1 = "tpch-sf1";
    static inline const std::string datasetTPCH10 = "tpch-sf10";
    static inline const std::set<std::string> realDatasets = {datasetOSM, datasetTaxi, datasetTPCH1, datasetTPCH10};

    // Partitioning schemes names
    static inline const std::string noPartition = "no-partition";
    static inline const std::string fixedGrid = "fixed-grid";
    static inline const std::string gridFile = "grid-file";
    static inline const std::string kdTree = "kd-tree";
    static inline const std::string strTree = "str-tree";
    static inline const std::string quadTree = "quad-tree";
    static inline const std::string hilbertCurve = "hilbert-curve";
    static inline const std::string zOrderCurve = "z-order-curve";

    // Folder names
    static inline const std::filesystem::path noPartitionFolder = std::filesystem::current_path() / noPartition;
    static inline const std::filesystem::path fixedGridFolder = std::filesystem::current_path() / fixedGrid;
    static inline const std::filesystem::path gridFileFolder = std::filesystem::current_path() / gridFile;
    static inline const std::filesystem::path kdTreeFolder = std::filesystem::current_path() / kdTree;
    static inline const std::filesystem::path strTreeFolder = std::filesystem::current_path() / strTree;
    static inline const std::filesystem::path quadTreeFolder = std::filesystem::current_path() / quadTree;
    static inline const std::filesystem::path hilbertCurveFolder = std::filesystem::current_path() / hilbertCurve;
    static inline const std::filesystem::path zOrderCurveFolder = std::filesystem::current_path() / zOrderCurve;
    static inline const std::filesystem::path testsFolder = std::filesystem::current_path();
    static inline const std::filesystem::path benchmarkFolder = testsFolder.parent_path() / "benchmark";
    static inline const std::filesystem::path datasetsFolder = benchmarkFolder / "datasets";

    // Partitioning config
    // Effect of partition size (= number of records per partition)
    const std::vector<int32_t> partitionSizes = {20000, 50000, 100000, 250000, 500000, 1000000};
    // Effect of dataset size
    const std::vector<int32_t> tpchScaleFactors = {1, 10, 20};
    static inline const std::string fileExtension = common::Settings::fileExtension;

    // Optional check of correctness
    static const bool checkCorrectness = true;
};

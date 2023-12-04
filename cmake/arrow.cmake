# Build the Arrow C++ libraries.
function(build_arrow)
    set(one_value_args)
    set(multi_value_args)

    cmake_parse_arguments(ARG
            "${options}"
            "${one_value_args}"
            "${multi_value_args}"
            ${ARGN})
    if (ARG_UNPARSED_ARGUMENTS)
        message(SEND_ERROR "Error: unrecognized arguments: ${ARG_UNPARSED_ARGUMENTS}")
    endif ()

    # If Arrow needs to be built, the default location will be within the build tree.
    set(ARROW_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/arrow_ep-prefix")

    set(ARROW_SHARED_LIBRARY_DIR "${ARROW_PREFIX}/lib")

    set(ARROW_SHARED_LIB_FILENAME
            "${CMAKE_SHARED_LIBRARY_PREFIX}arrow${CMAKE_SHARED_LIBRARY_SUFFIX}")
    set(ARROW_SHARED_LIB "${ARROW_SHARED_LIBRARY_DIR}/${ARROW_SHARED_LIB_FILENAME}")
    set(PARQUET_SHARED_LIB_FILENAME
            "${CMAKE_SHARED_LIBRARY_PREFIX}parquet${CMAKE_SHARED_LIBRARY_SUFFIX}")
    set(PARQUET_SHARED_LIB "${ARROW_SHARED_LIBRARY_DIR}/${PARQUET_SHARED_LIB_FILENAME}")

    set(ARROW_BINARY_DIR "${CMAKE_CURRENT_BINARY_DIR}/arrow_ep-build")
    set(ARROW_CMAKE_ARGS "-DCMAKE_INSTALL_PREFIX=${ARROW_PREFIX}"
            "-DCMAKE_INSTALL_LIBDIR=lib" "-Dxsimd_SOURCE=BUNDLED"
            "-DARROW_DEPENDENCY_SOURCE=BUNDLED" "-DARROW_LINK_SHARED=OFF"
            "-DARROW_BUILD_SHARED=OFF" "-DARROW_DEPENDENCY_USE_SHARED=OFF"
            "-DARROW_BUILD_STATIC=ON" "-DARROW_PARQUET=ON"
            "-DARROW_WITH_UTF8PROC=OFF" "-DARROW_WITH_RE2=ON"
            "-DARROW_FILESYSTEM=ON" "-DARROW_CSV=ON" "-DARROW_PYTHON=ON")

    set(ARROW_INCLUDE_DIR "${ARROW_PREFIX}/include")

    set(ARROW_BUILD_BYPRODUCTS "${ARROW_SHARED_LIB}" "${PARQUET_SHARED_LIB}")

    include(ExternalProject)

    externalproject_add(arrow_ep
            URL https://github.com/apache/arrow/archive/refs/tags/apache-arrow-9.0.0.tar.gz
            SOURCE_SUBDIR cpp
            BINARY_DIR "${ARROW_BINARY_DIR}"
            CMAKE_ARGS "${ARROW_CMAKE_ARGS}"
            BUILD_BYPRODUCTS "${ARROW_BUILD_BYPRODUCTS}")

    set(ARROW_LIBRARY_TARGET arrow_shared)
    set(PARQUET_LIBRARY_TARGET parquet_shared)

    file(MAKE_DIRECTORY "${ARROW_INCLUDE_DIR}")
    add_library(${ARROW_LIBRARY_TARGET} SHARED IMPORTED)
    add_library(${PARQUET_LIBRARY_TARGET} SHARED IMPORTED)
    set_target_properties(${ARROW_LIBRARY_TARGET}
            PROPERTIES INTERFACE_INCLUDE_DIRECTORIES ${ARROW_INCLUDE_DIR}
            IMPORTED_LOCATION ${ARROW_SHARED_LIB})
    set_target_properties(${PARQUET_LIBRARY_TARGET}
            PROPERTIES INTERFACE_INCLUDE_DIRECTORIES ${ARROW_INCLUDE_DIR}
            IMPORTED_LOCATION ${PARQUET_SHARED_LIB})

    add_dependencies(${ARROW_LIBRARY_TARGET} arrow_ep)
endfunction()

# ----------------------------------------------------------------------
# Thrift

macro(build_thrift)
if(CMAKE_VERSION VERSION_LESS 3.10)
message(FATAL_ERROR "Building thrift using ExternalProject requires at least CMake 3.10"
)
endif()
message("Building Apache Thrift from source")
set(THRIFT_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/thrift_ep-install")
set(THRIFT_INCLUDE_DIR "${THRIFT_PREFIX}/include")
set(THRIFT_CMAKE_ARGS
${EP_COMMON_CMAKE_ARGS}
"-DCMAKE_INSTALL_PREFIX=${THRIFT_PREFIX}"
"-DCMAKE_INSTALL_RPATH=${THRIFT_PREFIX}/lib"
-DBUILD_COMPILER=OFF
-DBUILD_SHARED_LIBS=OFF
-DBUILD_TESTING=OFF
-DBUILD_EXAMPLES=OFF
-DBUILD_TUTORIALS=OFF
-DWITH_QT4=OFF
-DWITH_C_GLIB=OFF
-DWITH_JAVA=OFF
-DWITH_PYTHON=OFF
-DWITH_HASKELL=OFF
-DWITH_CPP=ON
-DWITH_STATIC_LIB=ON
-DWITH_LIBEVENT=OFF
# Work around https://gitlab.kitware.com/cmake/cmake/issues/18865
-DBoost_NO_BOOST_CMAKE=ON)

# Thrift also uses boost. Forward important boost settings if there were ones passed.
if(DEFINED BOOST_ROOT)
list(APPEND THRIFT_CMAKE_ARGS "-DBOOST_ROOT=${BOOST_ROOT}")
endif()
if(DEFINED Boost_NAMESPACE)
list(APPEND THRIFT_CMAKE_ARGS "-DBoost_NAMESPACE=${Boost_NAMESPACE}")
endif()

set(THRIFT_STATIC_LIB_NAME "${CMAKE_STATIC_LIBRARY_PREFIX}thrift")
if(MSVC)
if(ARROW_USE_STATIC_CRT)
set(THRIFT_STATIC_LIB_NAME "${THRIFT_STATIC_LIB_NAME}mt")
list(APPEND THRIFT_CMAKE_ARGS "-DWITH_MT=ON")
else()
set(THRIFT_STATIC_LIB_NAME "${THRIFT_STATIC_LIB_NAME}md")
list(APPEND THRIFT_CMAKE_ARGS "-DWITH_MT=OFF")
endif()
endif()
if(${UPPERCASE_BUILD_TYPE} STREQUAL "DEBUG")
set(THRIFT_STATIC_LIB_NAME "${THRIFT_STATIC_LIB_NAME}d")
endif()
set(THRIFT_STATIC_LIB
"${THRIFT_PREFIX}/lib/${THRIFT_STATIC_LIB_NAME}${CMAKE_STATIC_LIBRARY_SUFFIX}")

if(BOOST_VENDORED)
set(THRIFT_DEPENDENCIES ${THRIFT_DEPENDENCIES} boost_ep)
endif()

externalproject_add(thrift_ep
URL ${THRIFT_SOURCE_URL}
URL_HASH "SHA256=${ARROW_THRIFT_BUILD_SHA256_CHECKSUM}"
BUILD_BYPRODUCTS "${THRIFT_STATIC_LIB}"
CMAKE_ARGS ${THRIFT_CMAKE_ARGS}
DEPENDS ${THRIFT_DEPENDENCIES} ${EP_LOG_OPTIONS})

add_library(thrift::thrift STATIC IMPORTED)
# The include directory must exist before it is referenced by a target.
file(MAKE_DIRECTORY "${THRIFT_INCLUDE_DIR}")
set_target_properties(thrift::thrift
PROPERTIES IMPORTED_LOCATION "${THRIFT_STATIC_LIB}"
INTERFACE_INCLUDE_DIRECTORIES "${THRIFT_INCLUDE_DIR}")
add_dependencies(toolchain thrift_ep)
add_dependencies(thrift::thrift thrift_ep)
set(THRIFT_VERSION ${ARROW_THRIFT_BUILD_VERSION})

list(APPEND ARROW_BUNDLED_STATIC_LIBS thrift::thrift)
endmacro()

if(ARROW_WITH_THRIFT)
# We already may have looked for Thrift earlier, when considering whether
# to build Boost, so don't look again if already found.
if(NOT Thrift_FOUND)
# Thrift c++ code generated by 0.13 requires 0.11 or greater
resolve_dependency(Thrift
REQUIRED_VERSION
0.11.0
PC_PACKAGE_NAMES
thrift)
endif()
# TODO: Don't use global includes but rather target_include_directories
include_directories(SYSTEM ${THRIFT_INCLUDE_DIR})
endif()

# ----------------------------------------------------------------------
# Protocol Buffers (required for ORC and Flight and Gandiva libraries)

macro(build_protobuf)
message("Building Protocol Buffers from source")
set(PROTOBUF_VENDORED TRUE)
set(PROTOBUF_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/protobuf_ep-install")
set(PROTOBUF_INCLUDE_DIR "${PROTOBUF_PREFIX}/include")
# Newer protobuf releases always have a lib prefix independent from CMAKE_STATIC_LIBRARY_PREFIX
set(PROTOBUF_STATIC_LIB
"${PROTOBUF_PREFIX}/lib/libprotobuf${CMAKE_STATIC_LIBRARY_SUFFIX}")
set(PROTOC_STATIC_LIB "${PROTOBUF_PREFIX}/lib/libprotoc${CMAKE_STATIC_LIBRARY_SUFFIX}")
set(Protobuf_PROTOC_LIBRARY "${PROTOC_STATIC_LIB}")
set(PROTOBUF_COMPILER "${PROTOBUF_PREFIX}/bin/protoc")

if(CMAKE_VERSION VERSION_LESS 3.7)
set(PROTOBUF_CONFIGURE_ARGS
"AR=${CMAKE_AR}"
"RANLIB=${CMAKE_RANLIB}"
"CC=${CMAKE_C_COMPILER}"
"CXX=${CMAKE_CXX_COMPILER}"
"--disable-shared"
"--prefix=${PROTOBUF_PREFIX}"
"CFLAGS=${EP_C_FLAGS}"
"CXXFLAGS=${EP_CXX_FLAGS}")
set(PROTOBUF_BUILD_COMMAND ${MAKE} ${MAKE_BUILD_ARGS})
if(CMAKE_OSX_SYSROOT)
list(APPEND PROTOBUF_CONFIGURE_ARGS "SDKROOT=${CMAKE_OSX_SYSROOT}")
list(APPEND PROTOBUF_BUILD_COMMAND "SDKROOT=${CMAKE_OSX_SYSROOT}")
endif()
set(PROTOBUF_EXTERNAL_PROJECT_ADD_ARGS
CONFIGURE_COMMAND
"./configure"
${PROTOBUF_CONFIGURE_ARGS}
BUILD_COMMAND
${PROTOBUF_BUILD_COMMAND})
else()
# Strip lto flags (which may be added by dh_auto_configure)
# See https://github.com/protocolbuffers/protobuf/issues/7092
set(PROTOBUF_C_FLAGS ${EP_C_FLAGS})
set(PROTOBUF_CXX_FLAGS ${EP_CXX_FLAGS})
string(REPLACE "-flto=auto" "" PROTOBUF_C_FLAGS "${PROTOBUF_C_FLAGS}")
string(REPLACE "-ffat-lto-objects" "" PROTOBUF_C_FLAGS "${PROTOBUF_C_FLAGS}")
string(REPLACE "-flto=auto" "" PROTOBUF_CXX_FLAGS "${PROTOBUF_CXX_FLAGS}")
string(REPLACE "-ffat-lto-objects" "" PROTOBUF_CXX_FLAGS "${PROTOBUF_CXX_FLAGS}")
set(PROTOBUF_CMAKE_ARGS
${EP_COMMON_CMAKE_ARGS}
-DBUILD_SHARED_LIBS=OFF
-DCMAKE_INSTALL_LIBDIR=lib
"-DCMAKE_INSTALL_PREFIX=${PROTOBUF_PREFIX}"
-Dprotobuf_BUILD_TESTS=OFF
-Dprotobuf_DEBUG_POSTFIX=
"-DCMAKE_C_FLAGS=${PROTOBUF_C_FLAGS}"
"-DCMAKE_CXX_FLAGS=${PROTOBUF_CXX_FLAGS}"
"-DCMAKE_C_FLAGS_${UPPERCASE_BUILD_TYPE}=${PROTOBUF_C_FLAGS}"
"-DCMAKE_CXX_FLAGS_${UPPERCASE_BUILD_TYPE}=${PROTOBUF_CXX_FLAGS}")
if(MSVC AND NOT ARROW_USE_STATIC_CRT)
list(APPEND PROTOBUF_CMAKE_ARGS "-Dprotobuf_MSVC_STATIC_RUNTIME=OFF")
endif()
if(ZLIB_ROOT)
list(APPEND PROTOBUF_CMAKE_ARGS "-DZLIB_ROOT=${ZLIB_ROOT}")
endif()
set(PROTOBUF_EXTERNAL_PROJECT_ADD_ARGS CMAKE_ARGS ${PROTOBUF_CMAKE_ARGS}
SOURCE_SUBDIR "cmake")
endif()

externalproject_add(protobuf_ep
${PROTOBUF_EXTERNAL_PROJECT_ADD_ARGS}
BUILD_BYPRODUCTS "${PROTOBUF_STATIC_LIB}" "${PROTOBUF_COMPILER}"
${EP_LOG_OPTIONS}
BUILD_IN_SOURCE 1
URL ${PROTOBUF_SOURCE_URL}
URL_HASH "SHA256=${ARROW_PROTOBUF_BUILD_SHA256_CHECKSUM}")

file(MAKE_DIRECTORY "${PROTOBUF_INCLUDE_DIR}")

add_library(arrow::protobuf::libprotobuf STATIC IMPORTED)
set_target_properties(arrow::protobuf::libprotobuf
PROPERTIES IMPORTED_LOCATION "${PROTOBUF_STATIC_LIB}"
INTERFACE_INCLUDE_DIRECTORIES
"${PROTOBUF_INCLUDE_DIR}")
add_library(arrow::protobuf::libprotoc STATIC IMPORTED)
set_target_properties(arrow::protobuf::libprotoc
PROPERTIES IMPORTED_LOCATION "${PROTOC_STATIC_LIB}"
INTERFACE_INCLUDE_DIRECTORIES
"${PROTOBUF_INCLUDE_DIR}")
add_executable(arrow::protobuf::protoc IMPORTED)
set_target_properties(arrow::protobuf::protoc PROPERTIES IMPORTED_LOCATION
"${PROTOBUF_COMPILER}")

add_dependencies(toolchain protobuf_ep)
add_dependencies(arrow::protobuf::libprotobuf protobuf_ep)

list(APPEND ARROW_BUNDLED_STATIC_LIBS arrow::protobuf::libprotobuf)
endmacro()

if(ARROW_WITH_PROTOBUF)
if(ARROW_WITH_GRPC)
# FlightSQL uses proto3 optionals, which require 3.15 or later.
set(ARROW_PROTOBUF_REQUIRED_VERSION "3.15.0")
elseif(ARROW_GANDIVA_JAVA)
# google::protobuf::MessageLite::ByteSize() is deprecated since
# Protobuf 3.4.0.
set(ARROW_PROTOBUF_REQUIRED_VERSION "3.4.0")
else()
set(ARROW_PROTOBUF_REQUIRED_VERSION "2.6.1")
endif()
resolve_dependency(Protobuf
REQUIRED_VERSION
${ARROW_PROTOBUF_REQUIRED_VERSION}
PC_PACKAGE_NAMES
protobuf)

if(ARROW_PROTOBUF_USE_SHARED AND MSVC_TOOLCHAIN)
add_definitions(-DPROTOBUF_USE_DLLS)
endif()

# TODO: Don't use global includes but rather target_include_directories
include_directories(SYSTEM ${PROTOBUF_INCLUDE_DIR})

if(TARGET arrow::protobuf::libprotobuf)
set(ARROW_PROTOBUF_LIBPROTOBUF arrow::protobuf::libprotobuf)
else()
# CMake 3.8 or older don't define the targets
if(NOT TARGET protobuf::libprotobuf)
add_library(protobuf::libprotobuf UNKNOWN IMPORTED)
set_target_properties(protobuf::libprotobuf
PROPERTIES IMPORTED_LOCATION "${PROTOBUF_LIBRARY}"
INTERFACE_INCLUDE_DIRECTORIES
"${PROTOBUF_INCLUDE_DIR}")
endif()
set(ARROW_PROTOBUF_LIBPROTOBUF protobuf::libprotobuf)
endif()
if(TARGET arrow::protobuf::libprotoc)
set(ARROW_PROTOBUF_LIBPROTOC arrow::protobuf::libprotoc)
else()
# CMake 3.8 or older don't define the targets
if(NOT TARGET protobuf::libprotoc)
if(PROTOBUF_PROTOC_LIBRARY AND NOT Protobuf_PROTOC_LIBRARY)
# Old CMake versions have a different casing.
set(Protobuf_PROTOC_LIBRARY ${PROTOBUF_PROTOC_LIBRARY})
endif()
if(NOT Protobuf_PROTOC_LIBRARY)
message(FATAL_ERROR "libprotoc was set to ${Protobuf_PROTOC_LIBRARY}")
endif()
add_library(protobuf::libprotoc UNKNOWN IMPORTED)
set_target_properties(protobuf::libprotoc
PROPERTIES IMPORTED_LOCATION "${Protobuf_PROTOC_LIBRARY}"
INTERFACE_INCLUDE_DIRECTORIES
"${PROTOBUF_INCLUDE_DIR}")
endif()
set(ARROW_PROTOBUF_LIBPROTOC protobuf::libprotoc)
endif()
if(TARGET arrow::protobuf::protoc)
set(ARROW_PROTOBUF_PROTOC arrow::protobuf::protoc)
else()
if(NOT TARGET protobuf::protoc)
add_executable(protobuf::protoc IMPORTED)
set_target_properties(protobuf::protoc PROPERTIES IMPORTED_LOCATION
"${PROTOBUF_PROTOC_EXECUTABLE}")
endif()
set(ARROW_PROTOBUF_PROTOC protobuf::protoc)
endif()

# Log protobuf paths as we often see issues with mixed sources for
# the libraries and protoc.
get_target_property(PROTOBUF_PROTOC_EXECUTABLE ${ARROW_PROTOBUF_PROTOC}
IMPORTED_LOCATION)
message(STATUS "Found protoc: ${PROTOBUF_PROTOC_EXECUTABLE}")
# Protobuf_PROTOC_LIBRARY is set by all versions of FindProtobuf.cmake
message(STATUS "Found libprotoc: ${Protobuf_PROTOC_LIBRARY}")
get_target_property(PROTOBUF_LIBRARY ${ARROW_PROTOBUF_LIBPROTOBUF} IMPORTED_LOCATION)
message(STATUS "Found libprotobuf: ${PROTOBUF_LIBRARY}")
message(STATUS "Found protobuf headers: ${PROTOBUF_INCLUDE_DIR}")
endif()
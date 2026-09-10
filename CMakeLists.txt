cmake_minimum_required(VERSION 3.16)
project(osh_change_index CXX C)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()

# ---------------------------------------------------------------------------
# System dependencies (install before configuring):
#   sudo apt install libosmium2-dev libprotozero-dev libexpat1-dev \
#                     libbz2-dev zlib1g-dev librocksdb-dev \
#                     libarrow-dev libparquet-dev
#
# H3 is fetched automatically via FetchContent, which exposes its public
# header as <h3api.h> (no "h3/" prefix) - confirmed by a real build.
# ---------------------------------------------------------------------------

find_path(OSMIUM_INCLUDE_DIR osmium/version.hpp)
if(NOT OSMIUM_INCLUDE_DIR)
  message(FATAL_ERROR "libosmium not found. Install libosmium2-dev or set OSMIUM_INCLUDE_DIR.")
endif()

find_package(EXPAT REQUIRED)
find_package(BZip2 REQUIRED)
find_package(ZLIB REQUIRED)

# protozero is header-only and does not reliably ship a pkg-config file on
# Debian, so it is located the same way as libosmium (find_path) rather
# than via pkg_check_modules.
find_path(PROTOZERO_INCLUDE_DIR protozero/pbf_reader.hpp)
if(NOT PROTOZERO_INCLUDE_DIR)
  message(FATAL_ERROR "protozero not found. Install libprotozero-dev or set PROTOZERO_INCLUDE_DIR.")
endif()

find_package(RocksDB REQUIRED)
find_package(Arrow REQUIRED)
find_package(Parquet REQUIRED)

include(FetchContent)
FetchContent_Declare(
  h3
  GIT_REPOSITORY https://github.com/uber/h3.git
  GIT_TAG v4.1.0
)
set(BUILD_TESTING OFF CACHE BOOL "" FORCE)
set(BUILD_BENCHMARKS OFF CACHE BOOL "" FORCE)
set(BUILD_FUZZERS OFF CACHE BOOL "" FORCE)
set(BUILD_FILTERS OFF CACHE BOOL "" FORCE)
set(BUILD_GENERATORS OFF CACHE BOOL "" FORCE)
set(ENABLE_DOCS OFF CACHE BOOL "" FORCE)
# ENABLE_DOCS=OFF did not fully suppress H3's Doxygen lookup in practice;
# the Dockerfile installs doxygen and graphviz (for the "dot" component)
# as a fallback so configure succeeds either way.
FetchContent_MakeAvailable(h3)

add_executable(osh_change_index
  src/main.cpp
  src/manifest.cpp
  src/parquet_batch_writer.cpp
  src/sort_pass.cpp
)

target_include_directories(osh_change_index PRIVATE
  ${OSMIUM_INCLUDE_DIR}
  ${PROTOZERO_INCLUDE_DIR}
  src
)

target_link_libraries(osh_change_index PRIVATE
  h3
  RocksDB::rocksdb
  Arrow::arrow_shared
  Parquet::parquet_shared
  EXPAT::EXPAT
  BZip2::BZip2
  ZLIB::ZLIB
  pthread
)

target_compile_options(osh_change_index PRIVATE -Wall -Wextra -O2)

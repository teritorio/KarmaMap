cmake_minimum_required(VERSION 3.16)
project(osh_change_index CXX C)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()

option(OSH_ENABLE_TESTS "Build the C++ test suite" ON)

# ---------------------------------------------------------------------------
# System dependencies (install before configuring):
#   sudo apt install libosmium2-dev libprotozero-dev libexpat1-dev \
#                     libbz2-dev zlib1g-dev libzstd-dev \
#                     libarrow-dev libarrow-compute-dev libparquet-dev \
#                     libgtest-dev
#
# H3 is fetched automatically via FetchContent, which exposes its public
# header as <h3api.h> (no "h3/" prefix).
# ---------------------------------------------------------------------------

find_path(OSMIUM_INCLUDE_DIR osmium/version.hpp)
if(NOT OSMIUM_INCLUDE_DIR)
  message(FATAL_ERROR "libosmium not found. Install libosmium2-dev or set OSMIUM_INCLUDE_DIR.")
endif()

find_package(EXPAT REQUIRED)
find_package(BZip2 REQUIRED)
find_package(ZLIB REQUIRED)

# zstd (like protozero/osmium: located manually rather than via CMake config,
# since Debian ships only a pkg-config file) compresses the node-cache blocks.
find_path(ZSTD_INCLUDE_DIR zstd.h)
if(NOT ZSTD_INCLUDE_DIR)
  message(FATAL_ERROR "libzstd not found. Install libzstd-dev or set ZSTD_INCLUDE_DIR.")
endif()
find_library(ZSTD_LIBRARY zstd)
if(NOT ZSTD_LIBRARY)
  message(FATAL_ERROR "libzstd not found. Install libzstd-dev or set ZSTD_LIBRARY.")
endif()

# protozero is header-only and does not reliably ship a pkg-config file on
# Debian, so it is located the same way as libosmium (find_path) rather
# than via pkg_check_modules.
find_path(PROTOZERO_INCLUDE_DIR protozero/pbf_reader.hpp)
if(NOT PROTOZERO_INCLUDE_DIR)
  message(FATAL_ERROR "protozero not found. Install libprotozero-dev or set PROTOZERO_INCLUDE_DIR.")
endif()

find_package(Arrow REQUIRED)
find_package(ArrowCompute REQUIRED)
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

add_library(osh_change_index_core STATIC
  src/manifest.cpp
  src/options.cpp
  src/parquet_batch_writer.cpp
  src/sort_pass.cpp
  src/user_indicators.cpp
)

target_include_directories(osh_change_index_core PUBLIC src)

# User-indicator scan (user_indicators.cpp) pulls in libosmium and protozero;
# made PUBLIC so the executable and the test binaries inherit them too.
target_include_directories(osh_change_index_core PUBLIC
  ${OSMIUM_INCLUDE_DIR}
  ${PROTOZERO_INCLUDE_DIR}
  ${ZSTD_INCLUDE_DIR}
)

target_link_libraries(osh_change_index_core PUBLIC
  h3
  Arrow::arrow_shared
  ArrowCompute::arrow_compute_shared
  Parquet::parquet_shared
  EXPAT::EXPAT
  BZip2::BZip2
  ZLIB::ZLIB
  ${ZSTD_LIBRARY}
)

target_compile_options(osh_change_index_core PRIVATE -Wall -Wextra -O2)

add_executable(osh_change_index src/main.cpp)

target_link_libraries(osh_change_index PRIVATE osh_change_index_core pthread)

if(OSH_ENABLE_TESTS)
  enable_testing()
  add_subdirectory(tests)
endif()

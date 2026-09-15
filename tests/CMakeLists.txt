# C++ tests. Google Test comes from Debian's libgtest-dev (found via CMake's
# built-in FindGTest module); no FetchContent is used for the test framework.
#
# Layout mirrors the two test binaries:
#   unit/ -> unit_tests : pure logic under src/ (h3_utils, date_utils,
#                         node_cache, user_indicator rules, options).
#   core/ -> core_tests : Parquet I/O and directory-processing integration
#                         (parquet_batch_writer, manifest, sort_pass,
#                         user_indicator finalize) with a custom main that
#                         initializes Arrow's compute engine.
# Linking osh_change_index_core (PUBLIC) pulls Arrow, Parquet, zstd, H3 and
# the user_indicators.cpp osmium includes transitively.

find_package(GTest REQUIRED)
include(GoogleTest)

add_executable(unit_tests
  unit/h3_utils_test.cpp
  unit/date_utils_test.cpp
  unit/node_cache_test.cpp
  unit/user_indicator_rules_test.cpp
  unit/reputation_distribution_test.cpp
  unit/options_test.cpp
)

target_compile_features(unit_tests PRIVATE cxx_std_17)
target_include_directories(unit_tests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(unit_tests PRIVATE
  GTest::GTest
  GTest::Main
  pthread
  osh_change_index_core
)

gtest_discover_tests(unit_tests)

add_executable(core_tests
  core/core_tests_main.cpp
  core/parquet_batch_writer_test.cpp
  core/manifest_test.cpp
  core/sort_pass_test.cpp
  core/user_indicator_finalize_test.cpp
  core/reputation_distribution_test.cpp
)

target_compile_features(core_tests PRIVATE cxx_std_17)
target_include_directories(core_tests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(core_tests PRIVATE
  GTest::GTest
  pthread
  osh_change_index_core
)

gtest_discover_tests(core_tests)
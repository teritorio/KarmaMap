#include <arrow/compute/api.h>
#include <gtest/gtest.h>

#include <iostream>

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);

    // Register Arrow's sort_indices/take compute kernels before the first
    // sort_pass test runs (sort_pass also initializes internally; this makes
    // the dependency explicit for any test that touches the compute engine).
    auto init_status = arrow::compute::Initialize();
    if (!init_status.ok()) {
        std::cerr << "Failed to initialize Arrow compute: "
                  << init_status.ToString() << std::endl;
        return 1;
    }
    return RUN_ALL_TESTS();
}
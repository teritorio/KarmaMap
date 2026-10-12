#include <gtest/gtest.h>

#include <filesystem>
#include <string>

#include "file_lock.hpp"

namespace {

std::filesystem::path test_root() {
    return std::filesystem::temp_directory_path() / "karmamap_file_lock_test";
}

std::string lock_path(const std::string& name) {
    const auto dir = test_root() / name;
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);
    return (dir / "karmamap.lock").string();
}

}  // namespace

class FileLockTest : public ::testing::Test {
protected:
    void TearDown() override { std::filesystem::remove_all(test_root()); }
};

TEST_F(FileLockTest, AcquireCreatesAndHolds) {
    const std::string path = lock_path("acquire");
    auto lock = FileLock::acquire(path, false);
    ASSERT_TRUE(lock.has_value());
    EXPECT_TRUE(std::filesystem::exists(path));
}

// flock treats two open() fds for the same file as independent, even within one
// process, so a second non-blocking acquire here must be refused.
TEST_F(FileLockTest, SecondNonBlockingAcquireFails) {
    const std::string path = lock_path("exclusive");
    auto first = FileLock::acquire(path, false);
    ASSERT_TRUE(first.has_value());
    auto second = FileLock::acquire(path, false);
    EXPECT_FALSE(second.has_value());
}

TEST_F(FileLockTest, ReleasedOnDestruction) {
    const std::string path = lock_path("release");
    {
        auto lock = FileLock::acquire(path, false);
        ASSERT_TRUE(lock.has_value());
    }
    auto again = FileLock::acquire(path, false);
    EXPECT_TRUE(again.has_value());
}

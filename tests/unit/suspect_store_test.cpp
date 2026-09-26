#include <gtest/gtest.h>

#include <cstdint>
#include <filesystem>
#include <stdexcept>
#include <string>
#include <vector>

#include "test_helpers.hpp"
#include "suspect_store.hpp"

namespace {

using test_helpers::TempDir;

TEST(SuspectStore, RoundtripSorted) {
    TempDir dir;
    const std::string path = dir.join("minutes.bin");
    {
        suspect_store::Writer w(path);
        w.add(3, 100, 5);
        w.add(3, 101, 7);
        w.add(10, 1440, 501);
        w.set_applied_seq(42);
        w.finish();
    }

    suspect_store::Reader r(path);
    EXPECT_EQ(r.size(), 3);
    EXPECT_EQ(r.applied_seq(), 42);
    EXPECT_EQ(r.uid_at(0), 3);
    EXPECT_EQ(r.minute_at(0), 100);
    EXPECT_EQ(r.count_at(0), 5);
    EXPECT_EQ(r.uid_at(1), 3);
    EXPECT_EQ(r.minute_at(1), 101);
    EXPECT_EQ(r.count_at(1), 7);
    EXPECT_EQ(r.uid_at(2), 10);
    EXPECT_EQ(r.minute_at(2), 1440);
    EXPECT_EQ(r.count_at(2), 501);
}

TEST(SuspectStore, LargeUidsAndNegativeOrdering) {
    // The sign-flipped byte encoding sorts exactly like the int64 uid: a
    // negative uid reads back earliest and the extreme INT64_MAX survives the
    // roundtrip. The writer enforces that input arrives already ascending.
    TempDir dir;
    const std::string path = dir.join("minutes.bin");
    {
        suspect_store::Writer w(path);
        w.add(-1, 5, 3);
        w.add(0, 5, 4);
        w.add(1, 0, 1);
        w.add(1, 5, 2);
        w.add(INT64_MAX, 9, 7);
        w.finish();
    }
    suspect_store::Reader r(path);
    ASSERT_EQ(r.size(), 5);
    EXPECT_EQ(r.uid_at(0), -1);
    EXPECT_EQ(r.uid_at(1), 0);
    EXPECT_EQ(r.uid_at(2), 1);
    EXPECT_EQ(r.uid_at(3), 1);
    EXPECT_EQ(r.uid_at(4), INT64_MAX);
    EXPECT_EQ(r.minute_at(4), 9);
    EXPECT_EQ(r.count_at(4), 7);
}

TEST(SuspectStore, MultiBlockRoundtrip) {
    TempDir dir;
    const std::string path = dir.join("big.bin");
    {
        suspect_store::Writer w(path);
        // More than one 2^18-record block, exactly crossing the boundary.
        constexpr uint32_t kBlock = 1 << 18;
        for (uint32_t i = 0; i < kBlock + 7; ++i) {
            w.add(1, i, i % 100);
        }
        w.finish();
    }
    suspect_store::Reader r(path);
    EXPECT_EQ(r.size(), (1u << 18) + 7);
    for (size_t i = 0; i < r.size(); ++i) {
        EXPECT_EQ(r.uid_at(i), 1);
        EXPECT_EQ(r.minute_at(i), static_cast<uint32_t>(i));
        EXPECT_EQ(r.count_at(i), static_cast<uint32_t>(i % 100));
    }
}

TEST(SuspectStore, RejectsNonAscendingInput) {
    TempDir dir;
    const std::string path = dir.join("dup.bin");
    {
        suspect_store::Writer w(path);
        w.add(1, 10, 1);
        EXPECT_THROW(w.add(1, 10, 2), std::runtime_error);  // duplicate key
    }
    {
        suspect_store::Writer w(path);
        w.add(1, 10, 1);
        EXPECT_THROW(w.add(0, 5, 2), std::runtime_error);  // uid goes backwards
    }
}

TEST(SuspectStore, FinishReplacesTheFileWhole) {
    TempDir dir;
    const std::string path = dir.join("r.bin");
    {
        suspect_store::Writer w(path);
        w.add(7, 0, 1);
        w.finish();
    }
    const uint64_t first_size = static_cast<uint64_t>(std::filesystem::file_size(path));
    {
        suspect_store::Writer w(path);
        w.add(7, 0, 1);
        w.add(9, 1, 2);
        w.finish();
    }
    // The write/rename swap in at finish(); no partial .tmp survives.
    EXPECT_FALSE(std::filesystem::exists(path + ".tmp"));
    EXPECT_NE(std::filesystem::file_size(path), 0);
    (void)first_size;
    suspect_store::Reader r(path);
    EXPECT_EQ(r.size(), 2);
}

TEST(SuspectStore, MissingFileAppliedSeqIsZero) {
    TempDir dir;
    EXPECT_EQ(suspect_store::applied_seq_of(dir.join("nope.bin")), 0);
}

TEST(SuspectStore, WriterOrderingAcrossRecordsPerUid) {
    // Same uid, ascending minutes is fine; same uid with a backward minute is
    // rejected even though the uid itself never changes.
    TempDir dir;
    const std::string path = dir.join("o.bin");
    {
        suspect_store::Writer w(path);
        w.add(5, 100, 1);
        EXPECT_THROW(w.add(5, 99, 2), std::runtime_error);
    }
}

}  // namespace
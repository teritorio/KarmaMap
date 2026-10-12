#pragma once

#include <cerrno>
#include <cstring>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>

#include <fcntl.h>
#include <sys/file.h>
#include <unistd.h>

// Advisory exclusive lock on a lock file, used to serialise karmamap runs that
// would otherwise modify the same data tree concurrently (e.g. overlapping cron
// updates). The lock is held while the object lives and released on destruction.
class FileLock {
public:
    // Opens (creating) `path` and takes an exclusive flock(2). With block=false
    // the call returns std::nullopt when another process already holds the
    // lock, so a cron run exits instead of piling up; with block=true it waits.
    // Throws std::runtime_error on any other failure.
    static std::optional<FileLock> acquire(const std::string& path, bool block = false) {
        const int fd = ::open(path.c_str(), O_RDWR | O_CREAT | O_CLOEXEC, 0644);
        if (fd < 0) {
            throw std::runtime_error("Failed to open lock file " + path + ": " +
                                     std::strerror(errno));
        }
        const int op = LOCK_EX | (block ? 0 : LOCK_NB);
        if (::flock(fd, op) != 0) {
            const int err = errno;
            ::close(fd);
            if (!block && err == EWOULDBLOCK) return std::nullopt;
            throw std::runtime_error("Failed to lock " + path + ": " +
                                     std::strerror(err));
        }
        return FileLock(fd);
    }

    FileLock(FileLock&& other) noexcept : fd_(other.fd_) { other.fd_ = -1; }

    FileLock& operator=(FileLock&& other) noexcept {
        if (this != &other) {
            release();
            fd_ = other.fd_;
            other.fd_ = -1;
        }
        return *this;
    }

    FileLock(const FileLock&) = delete;
    FileLock& operator=(const FileLock&) = delete;

    ~FileLock() { release(); }

private:
    explicit FileLock(int fd) : fd_(fd) {}

    void release() {
        if (fd_ < 0) return;
        ::flock(fd_, LOCK_UN);
        ::close(fd_);
        fd_ = -1;
    }

    int fd_ = -1;
};

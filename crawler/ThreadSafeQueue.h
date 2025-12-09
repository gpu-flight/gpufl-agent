#pragma once
#include <mutex>
#include <condition_variable>
#include <deque>
#include <cstddef>

// A minimal thread-safe unbounded FIFO queue with close() semantics.
// pop() blocks until an item is available or the queue is closed.
// When closed and drained, pop() returns false.
template <typename T>
class ThreadSafeQueue {
public:
    // capacity = 0 means unbounded
    explicit ThreadSafeQueue(std::size_t capacity = 0)
        : capacity_(capacity) {}
    ThreadSafeQueue(const ThreadSafeQueue&) = delete;
    ThreadSafeQueue& operator=(const ThreadSafeQueue&) = delete;

    void push(const T& value) {
        std::unique_lock<std::mutex> lk(m_);
        // If bounded, wait until space is available or closed
        if (capacity_ > 0) {
            cv_.wait(lk, [&]{ return closed_ || q_.size() < capacity_; });
            if (closed_) return; // discard pushes after close
        } else {
            if (closed_) return; // discard pushes after close
        }
        q_.push_back(value);
        lk.unlock();
        cv_.notify_one(); // wake a consumer
    }

    void push(T&& value) {
        std::unique_lock<std::mutex> lk(m_);
        if (capacity_ > 0) {
            cv_.wait(lk, [&]{ return closed_ || q_.size() < capacity_; });
            if (closed_) return;
        } else {
            if (closed_) return;
        }
        q_.push_back(std::move(value));
        lk.unlock();
        cv_.notify_one();
    }

    // Blocks until an item is available or the queue is closed.
    // Returns true if an item was popped, false if closed and empty.
    bool pop(T& out) {
        std::unique_lock<std::mutex> lk(m_);
        cv_.wait(lk, [&]{ return closed_ || !q_.empty(); });
        if (q_.empty()) return false; // closed and drained
        out = std::move(q_.front());
        q_.pop_front();
        lk.unlock();
        cv_.notify_one(); // wake a producer waiting for space
        return true;
    }

    void close() {
        {
            std::lock_guard<std::mutex> lk(m_);
            closed_ = true;
        }
        cv_.notify_all();
    }

    bool empty() const {
        std::lock_guard<std::mutex> lk(m_);
        return q_.empty();
    }

private:
    mutable std::mutex m_;
    std::condition_variable cv_;
    std::deque<T> q_;
    bool closed_ {false};
    std::size_t capacity_ {0};
};

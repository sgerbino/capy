//
// Copyright (c) 2026 Steve Gerbino
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//
// Official repository: https://github.com/cppalliance/capy
//

//
// Minimal thread pool for benchmarks.
//
// Provides intrusive_queue, work_item, and bench_thread_pool
// shared across benchmark files. The pool enqueues work_items
// without per-operation allocation.
//

#ifndef BOOST_CAPY_BENCH_POOL_HPP
#define BOOST_CAPY_BENCH_POOL_HPP

#include <atomic>
#include <condition_variable>
#include <coroutine>
#include <cstddef>
#include <mutex>
#include <thread>
#include <utility>
#include <vector>

template <typename T>
class intrusive_queue
{
public:
    class node
    {
        friend class intrusive_queue;
        T* next_;
    };

private:
    T* head_ = nullptr;
    T* tail_ = nullptr;

public:
    intrusive_queue() = default;
    intrusive_queue(intrusive_queue const&) = delete;
    intrusive_queue& operator=(intrusive_queue const&) = delete;

    bool empty() const noexcept { return head_ == nullptr; }

    void push(T* w) noexcept
    {
        w->next_ = nullptr;
        if (tail_)
            tail_->next_ = w;
        else
            head_ = w;
        tail_ = w;
    }

    T* pop() noexcept
    {
        if (!head_)
            return nullptr;
        T* w = head_;
        head_ = head_->next_;
        if (!head_)
            tail_ = nullptr;
        return w;
    }
};

struct work_item : intrusive_queue<work_item>::node
{
    virtual void execute() noexcept = 0;
protected:
    ~work_item() = default;
};

class bench_thread_pool
{
    struct coro_work : work_item
    {
        std::coroutine_handle<> h_;

        explicit coro_work(std::coroutine_handle<> h) noexcept : h_(h) {}

        void execute() noexcept override
        {
            auto h = h_;
            delete this;
            h.resume();
        }
    };

    std::mutex mutex_;
    std::condition_variable cv_;
    intrusive_queue<work_item> q_;
    std::vector<std::thread> threads_;
    std::atomic<std::size_t> outstanding_work_{0};
    bool stop_{false};
    bool joined_{false};
    std::size_t num_threads_;
    std::once_flag start_flag_;

    void ensure_started()
    {
        std::call_once(start_flag_, [this] {
            threads_.reserve(num_threads_);
            for (std::size_t i = 0; i < num_threads_; ++i)
                threads_.emplace_back([this] { run(); });
        });
    }

    void run()
    {
        for (;;)
        {
            work_item* w = nullptr;
            {
                std::unique_lock lock(mutex_);
                cv_.wait(lock, [this] {
                    return !q_.empty() || stop_;
                });
                if (stop_)
                    return;
                w = q_.pop();
            }
            if (w)
                w->execute();
        }
    }

public:
    explicit bench_thread_pool(std::size_t num_threads = 0)
        : num_threads_(num_threads == 0
            ? (std::max)(std::thread::hardware_concurrency(), 1u)
            : num_threads)
    {}

    ~bench_thread_pool()
    {
        stop();
        join();
    }

    bench_thread_pool(bench_thread_pool const&) = delete;
    bench_thread_pool& operator=(bench_thread_pool const&) = delete;

    void enqueue(work_item* w)
    {
        ensure_started();
        {
            std::lock_guard lock(mutex_);
            q_.push(w);
        }
        cv_.notify_one();
    }

    void post(std::coroutine_handle<> h)
    {
        enqueue(new coro_work(h));
    }

    void on_work_started() noexcept
    {
        outstanding_work_.fetch_add(1, std::memory_order_acq_rel);
    }

    void on_work_finished() noexcept
    {
        if (outstanding_work_.fetch_sub(
            1, std::memory_order_acq_rel) == 1)
        {
            std::lock_guard lock(mutex_);
            if (joined_ && !stop_)
                stop_ = true;
            cv_.notify_all();
        }
    }

    void join() noexcept
    {
        {
            std::unique_lock lock(mutex_);
            if (joined_)
                return;
            joined_ = true;

            if (outstanding_work_.load(
                std::memory_order_acquire) == 0)
            {
                stop_ = true;
                cv_.notify_all();
            }
            else
            {
                cv_.wait(lock, [this] { return stop_; });
            }
        }

        for (auto& t : threads_)
            if (t.joinable())
                t.join();
    }

    void stop() noexcept
    {
        {
            std::lock_guard lock(mutex_);
            stop_ = true;
        }
        cv_.notify_all();
    }
};

#endif

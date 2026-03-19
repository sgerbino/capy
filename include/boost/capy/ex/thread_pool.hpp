//
// Copyright (c) 2025 Vinnie Falco (vinnie.falco@gmail.com)
// Copyright (c) 2026 Michael Vandeberg
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//
// Official repository: https://github.com/cppalliance/capy
//

#ifndef BOOST_CAPY_EX_THREAD_POOL_HPP
#define BOOST_CAPY_EX_THREAD_POOL_HPP

#include <boost/capy/detail/config.hpp>
#include <boost/capy/detail/intrusive.hpp>
#include <boost/capy/detail/work_item.hpp>
#include <boost/capy/ex/execution_context.hpp>
#include <atomic>
#include <condition_variable>
#include <coroutine>
#include <cstddef>
#include <mutex>
#include <string_view>
#include <thread>
#include <vector>

namespace boost {
namespace capy {

/** A pool of threads for executing work concurrently.

    Use this when you need to run coroutines on multiple threads
    without the overhead of creating and destroying threads for
    each task. Work items are distributed across the pool using
    a shared queue.

    @par Thread Safety
    Distinct objects: Safe.
    Shared objects: Unsafe.

    @par Example
    @code
    thread_pool pool(4);  // 4 worker threads
    auto ex = pool.get_executor();
    ex.post(some_coroutine);
    // pool destructor waits for all work to complete
    @endcode
*/
class BOOST_CAPY_DECL
    thread_pool
    : public execution_context
{
    // Heap-allocated wrapper for raw coroutine handles
    struct coro_work : work_item
    {
        std::coroutine_handle<> h_;

        explicit coro_work(std::coroutine_handle<> h) noexcept
            : h_(h)
        {
        }

        void execute() noexcept override
        {
            auto h = h_;
            delete this;
            h.resume();
        }
    };

    std::mutex mutex_;
    std::condition_variable cv_;
    detail::intrusive_queue<work_item> q_;
    std::vector<std::thread> threads_;
    std::atomic<std::size_t> outstanding_work_{0};
    bool stop_{false};
    bool joined_{false};
    std::size_t num_threads_;
    char thread_name_prefix_[13]{};
    std::once_flag start_flag_;

    void ensure_started()
    {
        std::call_once(start_flag_, [this]{
            threads_.reserve(num_threads_);
            for(std::size_t i = 0; i < num_threads_; ++i)
                threads_.emplace_back([this, i]{ run(i); });
        });
    }

    BOOST_CAPY_DECL void run(std::size_t index);

public:
    class executor_type;

    /** Destroy the thread pool.

        Signals all worker threads to stop, waits for them to
        finish, and destroys any pending work items.
    */
    BOOST_CAPY_DECL ~thread_pool();

    /** Construct a thread pool.

        Creates a pool with the specified number of worker threads.
        If `num_threads` is zero, the number of threads is set to
        the hardware concurrency, or one if that cannot be determined.

        @param num_threads The number of worker threads, or zero
            for automatic selection.

        @param thread_name_prefix The prefix for worker thread names.
            Thread names appear as "{prefix}0", "{prefix}1", etc.
            The prefix is truncated to 12 characters. Defaults to
            "capy-pool-".
    */
    BOOST_CAPY_DECL explicit
    thread_pool(
        std::size_t num_threads = 0,
        std::string_view thread_name_prefix = "capy-pool-");

    thread_pool(thread_pool const&) = delete;
    thread_pool& operator=(thread_pool const&) = delete;

    /** Wait for all outstanding work to complete.

        Releases the internal work guard, then blocks the calling
        thread until all outstanding work tracked by
        @ref executor_type::on_work_started and
        @ref executor_type::on_work_finished completes. After all
        work finishes, joins the worker threads.

        If @ref stop is called while `join()` is blocking, the
        pool stops without waiting for remaining work to
        complete. Worker threads finish their current item and
        exit; `join()` still waits for all threads to be joined
        before returning.

        This function is idempotent. The first call performs the
        join; subsequent calls return immediately.

        @par Preconditions
        Must not be called from a thread in this pool (undefined
        behavior).

        @par Postconditions
        All worker threads have been joined. The pool cannot be
        reused.

        @par Thread Safety
        May be called from any thread not in this pool.
    */
    BOOST_CAPY_DECL void join() noexcept;

    /** Request all worker threads to stop.

        Signals all threads to exit after finishing their current
        work item. Queued work that has not started is abandoned.
        Does not wait for threads to exit.

        If @ref join is blocking on another thread, calling
        `stop()` causes it to stop waiting for outstanding
        work. The `join()` call still waits for worker threads
        to finish their current item and exit before returning.
    */
    BOOST_CAPY_DECL void stop() noexcept;

    /** Enqueue an inline work item without allocation.

        The work item is enqueued directly into the pool's work
        queue. No heap allocation occurs. The work item must
        remain alive from this call until its
        @ref work_item::execute method returns.

        @param w The work item to enqueue.

        @see work_item
    */
    void
    enqueue(work_item* w)
    {
        ensure_started();
        {
            std::lock_guard<std::mutex> lock(mutex_);
            q_.push(w);
        }
        cv_.notify_one();
    }

    /** Post a coroutine to the thread pool.

        Wraps the handle in a heap-allocated work item and
        enqueues it. Prefer @ref enqueue with a @ref work_item
        subclass to avoid this allocation.

        @param h The coroutine handle to execute.
    */
    void
    post(std::coroutine_handle<> h)
    {
        enqueue(new coro_work(h));
    }

    /** Notify that work has started.

        Increments the outstanding work count.
    */
    void
    on_work_started() noexcept
    {
        outstanding_work_.fetch_add(1, std::memory_order_acq_rel);
    }

    /** Notify that work has finished.

        Decrements the outstanding work count. When the count
        reaches zero after @ref join has been called, the pool's
        worker threads are signaled to stop.
    */
    void
    on_work_finished() noexcept
    {
        if(outstanding_work_.fetch_sub(
            1, std::memory_order_acq_rel) == 1)
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if(joined_ && !stop_)
                stop_ = true;
            cv_.notify_all();
        }
    }

    /** Return an executor for this thread pool.

        @return An executor associated with this thread pool.
    */
    inline executor_type
    get_executor() const noexcept;
};

/** An executor that submits work to a thread_pool.

    Executors are lightweight handles that can be copied and stored.
    All copies refer to the same underlying thread pool.

    @par Thread Safety
    Distinct objects: Safe.
    Shared objects: Safe.
*/
class thread_pool::executor_type
{
    friend class thread_pool;

    thread_pool* pool_ = nullptr;

    explicit
    executor_type(thread_pool& pool) noexcept
        : pool_(&pool)
    {
    }

public:
    /// Construct a default null executor.
    executor_type() = default;

    /// Return the underlying thread pool.
    thread_pool&
    context() const noexcept
    {
        return *pool_;
    }

    /// Notify that work has started.
    void
    on_work_started() const noexcept
    {
        pool_->on_work_started();
    }

    /// Notify that work has finished.
    void
    on_work_finished() const noexcept
    {
        pool_->on_work_finished();
    }

    /** Dispatch a coroutine for execution.

        Posts the coroutine to the thread pool for execution on a
        worker thread and returns `std::noop_coroutine()`. Thread
        pools never execute inline because no single thread "owns"
        the pool.

        @param h The coroutine handle to execute.

        @return `std::noop_coroutine()` always.
    */
    std::coroutine_handle<>
    dispatch(std::coroutine_handle<> h) const
    {
        post(h);
        return std::noop_coroutine();
    }

    /** Post a coroutine to the thread pool.

        @param h The coroutine handle to execute.
    */
    void
    post(std::coroutine_handle<> h) const
    {
        pool_->post(h);
    }

    /** Enqueue an inline work item without allocation.

        @param w The work item to enqueue.

        @see work_item
    */
    void
    enqueue(work_item* w) const
    {
        pool_->enqueue(w);
    }

    /// Return true if two executors refer to the same thread pool.
    bool
    operator==(executor_type const& other) const noexcept
    {
        return pool_ == other.pool_;
    }
};

inline thread_pool::executor_type
thread_pool::
get_executor() const noexcept
{
    return executor_type(
        const_cast<thread_pool&>(*this));
}

} // capy
} // boost

#endif

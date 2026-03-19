//
// Copyright (c) 2026 Steve Gerbino
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//
// Official repository: https://github.com/cppalliance/capy
//

//
// Null Stream Benchmark
//
// Measures the per-operation overhead of co_awaiting read_some on a
// null stream that does no I/O — it just posts the coroutine back to
// the executor. This isolates the async machinery cost from actual
// I/O work.
//
// Two implementations are compared:
//   - capy:  IoAwaitable with await_suspend posting the coroutine handle
//   - beman: beman::execution::task with as_awaitable returning an
//            inline work item (zero allocation per read)
//

#include <beman/execution/execution.hpp>
#include <beman/task/task.hpp>
#include <boost/capy.hpp>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <coroutine>
#include <cstdio>
#include <memory_resource>
#include <mutex>
#include <thread>
#include <utility>
#include <vector>

namespace ex = beman::execution;
namespace capy = boost::capy;

// ===================================================================
// capy benchmark
// ===================================================================

struct capy_null_stream
{
    struct awaitable
    {
        bool await_ready() const noexcept { return false; }

        void await_suspend(
            std::coroutine_handle<> h,
            capy::io_env const* env)
        {
            env->executor.post(h);
        }

        capy::io_result<std::size_t> await_resume()
        {
            return {{}, 0};
        }
    };

    awaitable read_some(capy::mutable_buffer)
    {
        return {};
    }
};

capy::task<> capy_benchmark()
{
    capy_null_stream stream;
    char buf[64];

    auto start = std::chrono::steady_clock::now();

    for (int i = 0; i < 10'000; ++i)
        (void)co_await stream.read_some(capy::mutable_buffer(buf, sizeof(buf)));

    auto elapsed = std::chrono::steady_clock::now() - start;
    auto us = std::chrono::duration_cast<std::chrono::microseconds>(elapsed).count();
    std::printf("capy:  10,000 read_some calls in %lld us\n",
        static_cast<long long>(us));
}

// ===================================================================
// inlined thread pool for beman benchmark
// ===================================================================

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

// Base for work items that can be directly enqueued without allocation
struct work_item : intrusive_queue<work_item>::node
{
    virtual void execute() noexcept = 0;
protected:
    ~work_item() = default;
};

class bench_thread_pool
{
    // Heap-allocated wrapper for raw coroutine handles
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
                cv_.wait(lock, [this] { return !q_.empty() || stop_; });
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
        if (outstanding_work_.fetch_sub(1, std::memory_order_acq_rel) == 1)
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

            if (outstanding_work_.load(std::memory_order_acquire) == 0)
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

// ===================================================================
// beman::execution infrastructure
// ===================================================================

struct sender_executor
{
    bench_thread_pool* pool_ = nullptr;

    void post(auto&& f) const { pool_->post(std::forward<decltype(f)>(f)); }
    void enqueue(work_item* w) const { pool_->enqueue(w); }
    auto dispatch(auto&& f) const
    {
        pool_->post(std::forward<decltype(f)>(f));
        return std::noop_coroutine();
    }

    bool operator==(sender_executor const&) const noexcept = default;
};

struct pool_scheduler
{
    using scheduler_concept = ex::scheduler_t;

    sender_executor ex_;

    struct env
    {
        sender_executor ex_;
        auto query(ex::get_completion_scheduler_t<ex::set_value_t> const&) const noexcept
        {
            return pool_scheduler{ex_};
        }
    };

    template <ex::receiver Receiver>
    struct op_state : work_item
    {
        using operation_state_concept = ex::operation_state_t;

        std::remove_cvref_t<Receiver> rcvr_;
        sender_executor ex_;

        op_state(Receiver rcvr, sender_executor ex)
            : rcvr_(std::move(rcvr))
            , ex_(ex)
        {}

        op_state(op_state const&) = delete;
        op_state(op_state&&) = delete;
        op_state& operator=(op_state const&) = delete;
        op_state& operator=(op_state&&) = delete;

        void execute() noexcept override
        {
            ex::set_value(std::move(rcvr_));
        }

        void start() & noexcept
        {
            ex_.enqueue(this);
        }
    };

    struct sender
    {
        using sender_concept = ex::sender_t;
        using completion_signatures =
            ex::completion_signatures<ex::set_value_t()>;

        sender_executor ex_;

        auto get_env() const noexcept { return env{ex_}; }

        template <ex::receiver Receiver>
        auto connect(Receiver&& rcvr) -> op_state<std::remove_cvref_t<Receiver>>
        {
            return {std::forward<Receiver>(rcvr), ex_};
        }
    };

    auto schedule() -> sender { return {ex_}; }
    bool operator==(pool_scheduler const&) const = default;
};

struct get_sender_executor_t
{
    constexpr bool query(ex::forwarding_query_t const&) const noexcept { return true; }

    template <typename Env>
        requires requires(Env const& env) {
            env.query(std::declval<get_sender_executor_t const&>());
        }
    auto operator()(Env const& env) const noexcept
    {
        return env.query(*this);
    }
};
inline constexpr get_sender_executor_t get_sender_executor{};

struct io_env
{
    using scheduler_type = pool_scheduler;
    using allocator_type = std::pmr::polymorphic_allocator<std::byte>;

    sender_executor executor;

    auto query(get_sender_executor_t const&) const noexcept -> sender_executor
    {
        return executor;
    }

    io_env() = default;

    template <typename Env>
        requires requires(Env const& e) { pool_scheduler{ex::get_scheduler(e)}; }
    io_env(Env const& e)
        : executor(pool_scheduler{ex::get_scheduler(e)}.ex_)
    {}
};

template <typename T = void>
using io_task = ex::task<T, io_env>;

// ===================================================================
// beman benchmark
// ===================================================================

struct beman_null_stream
{
    bench_thread_pool* pool_;

    struct read_awaitable : work_item
    {
        bench_thread_pool* pool_;
        std::coroutine_handle<> h_{};

        explicit read_awaitable(bench_thread_pool* p) noexcept : pool_(p) {}

        bool await_ready() const noexcept { return false; }

        void await_suspend(std::coroutine_handle<> h)
        {
            h_ = h;
            pool_->enqueue(this);
        }

        std::size_t await_resume() noexcept { return 0; }

        void execute() noexcept override
        {
            h_.resume();
        }
    };

    struct read_sender
    {
        using sender_concept = ex::sender_t;
        using completion_signatures =
            ex::completion_signatures<ex::set_value_t(std::size_t)>;

        bench_thread_pool* pool_;

        template <typename Promise>
        auto as_awaitable(Promise&) -> read_awaitable
        {
            return read_awaitable{pool_};
        }

        template <ex::receiver Receiver>
        struct op_state : work_item
        {
            using operation_state_concept = ex::operation_state_t;

            std::remove_cvref_t<Receiver> rcvr_;
            bench_thread_pool* pool_;

            op_state(Receiver rcvr, bench_thread_pool* pool)
                : rcvr_(std::move(rcvr))
                , pool_(pool)
            {}

            op_state(op_state const&) = delete;
            op_state(op_state&&) = delete;
            op_state& operator=(op_state const&) = delete;
            op_state& operator=(op_state&&) = delete;

            void execute() noexcept override
            {
                ex::set_value(std::move(rcvr_), std::size_t{0});
            }

            void start() & noexcept
            {
                pool_->enqueue(this);
            }
        };

        template <ex::receiver Receiver>
        auto connect(Receiver&& rcvr) -> op_state<std::remove_cvref_t<Receiver>>
        {
            return {std::forward<Receiver>(rcvr), pool_};
        }
    };

    read_sender read_some(auto)
    {
        return {pool_};
    }
};

auto beman_benchmark(
    bench_thread_pool* pool,
    std::allocator_arg_t,
    std::pmr::polymorphic_allocator<std::byte>) -> io_task<>
{
    beman_null_stream stream{pool};
    char buf[64];

    auto start = std::chrono::steady_clock::now();

    for (int i = 0; i < 10'000; ++i)
        (void)co_await stream.read_some(buf);

    auto elapsed = std::chrono::steady_clock::now() - start;
    auto us = std::chrono::duration_cast<std::chrono::microseconds>(elapsed).count();
    std::printf("beman: 10,000 read_some calls in %lld us\n",
        static_cast<long long>(us));
}

// ===================================================================
// main
// ===================================================================

int main()
{
    {
        capy::thread_pool pool(1);
        capy::run_async(pool.get_executor())(capy_benchmark());
        pool.join();
    }

    {
        bench_thread_pool pool(1);
        pool_scheduler sched{sender_executor{&pool}};
        auto* mr = capy::get_recycling_memory_resource();
        ex::sync_wait(ex::starts_on(sched,
            beman_benchmark(&pool, std::allocator_arg,
                std::pmr::polymorphic_allocator<std::byte>(mr))));
        pool.join();
    }

    return 0;
}

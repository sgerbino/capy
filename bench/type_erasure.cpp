//
// Copyright (c) 2026 Steve Gerbino
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//
// Official repository: https://github.com/cppalliance/capy
//

//
// Type Erasure Benchmark
//
// Measures per-operation overhead of three approaches to type-erased
// async I/O through a null stream (no real I/O, just posts to the
// executor). 100M read_some calls per column, single thread.
//
// Column A — Awaitable + virtual stream
//   Virtual base returns an awaitable. coroutine_handle<> is already
//   type-erased, so no per-operation allocation is needed.
//   One session instantiation.
//
// Column B — Sender, monomorphized (control)
//   Concrete stream, template session. Zero allocation but requires
//   one instantiation per stream type.
//
// Column C — Sender + type-erased (any_read_sender)
//   Virtual base returns any_read_sender. connect() must heap-allocate
//   the operation state because its type is erased.
//   One session instantiation, one allocation per read_some.
//

#include "beman_env.hpp"
#include <boost/capy.hpp>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <memory>

namespace ex = beman::execution;
namespace capy = boost::capy;

using bench::sender_executor;
using bench::pool_scheduler;
using bench::io_task;

// ===================================================================
// allocation counter
// ===================================================================

static std::atomic<int64_t> g_alloc_count{0};

void* operator new(std::size_t n)
{
    g_alloc_count.fetch_add(1, std::memory_order_relaxed);
    void* p = std::malloc(n);
    if (!p)
        throw std::bad_alloc();
    return p;
}

void operator delete(void* p) noexcept
{
    std::free(p);
}

void operator delete(void* p, std::size_t) noexcept
{
    std::free(p);
}

// ===================================================================
// Column A — Awaitable + virtual stream
//
// stream_base has a virtual read_some that returns a read_op.
// The read_op provides as_awaitable, which returns an awaitable
// that embeds a work_item — zero per-operation allocation.
// The session function takes stream_base& — one instantiation.
// ===================================================================

struct read_awaitable_a : work_item
{
    bench_thread_pool* pool_;
    std::coroutine_handle<> h_{};

    explicit read_awaitable_a(bench_thread_pool* p) noexcept : pool_(p) {}

    bool await_ready() const noexcept { return false; }

    void await_suspend(std::coroutine_handle<> h)
    {
        h_ = h;
        pool_->enqueue(this);
    }

    std::size_t await_resume() noexcept { return 0; }

    void execute() noexcept override { h_.resume(); }
};

struct read_op_a
{
    bench_thread_pool* pool_;

    template <typename Promise>
    auto as_awaitable(Promise&) -> read_awaitable_a
    {
        return read_awaitable_a{pool_};
    }
};

struct stream_base
{
    virtual read_op_a read_some(capy::mutable_buffer) = 0;
    virtual ~stream_base() = default;
};

struct noop_stream_a : stream_base
{
    bench_thread_pool* pool_;

    explicit noop_stream_a(bench_thread_pool* p) noexcept : pool_(p) {}

    read_op_a read_some(capy::mutable_buffer) override
    {
        return {pool_};
    }
};

capy::task<> inner_a(stream_base& stream)
{
    char buf[64];
    for (int i = 0; i < 10'000; ++i)
        (void)co_await stream.read_some(
            capy::mutable_buffer(buf, sizeof(buf)));
}

capy::task<> benchmark_a(bench_thread_pool& bpool)
{
    noop_stream_a stream{&bpool};

    auto before = g_alloc_count.load(std::memory_order_relaxed);
    auto start = std::chrono::steady_clock::now();

    for (int i = 0; i < 10'000; ++i)
        co_await inner_a(stream);

    auto elapsed = std::chrono::steady_clock::now() - start;
    auto after = g_alloc_count.load(std::memory_order_relaxed);
    auto us = std::chrono::duration_cast<
        std::chrono::microseconds>(elapsed).count();
    auto allocs = after - before;

    std::printf(
        "A  awaitable + virtual    %10lld us  %5.1f ns/op  "
        "%lld allocs (%4.2f/op)\n",
        static_cast<long long>(us),
        static_cast<double>(us) * 1000.0 / 100'000'000.0,
        static_cast<long long>(allocs),
        static_cast<double>(allocs) / 100'000'000.0);
}

// ===================================================================
// Column B — Sender, monomorphized (control)
//
// Concrete stream, template session function. The sender's op_state
// is inline (embeds work_item). Zero per-operation allocation, but
// each stream type requires a separate session instantiation.
// ===================================================================

struct noop_read_sender
{
    using sender_concept = ex::sender_t;
    using completion_signatures =
        ex::completion_signatures<ex::set_value_t(std::size_t)>;

    bench_thread_pool* pool_;

    // awaitable path (used when co_awaited from io_task via as_awaitable)
    template <typename Promise>
    struct awaitable : work_item
    {
        bench_thread_pool* pool_;
        std::coroutine_handle<> h_{};

        explicit awaitable(bench_thread_pool* p) noexcept : pool_(p) {}

        bool await_ready() const noexcept { return false; }

        void await_suspend(std::coroutine_handle<> h)
        {
            h_ = h;
            pool_->enqueue(this);
        }

        std::size_t await_resume() noexcept { return 0; }

        void execute() noexcept override { h_.resume(); }
    };

    template <typename Promise>
    auto as_awaitable(Promise&) -> awaitable<Promise>
    {
        return awaitable<Promise>{pool_};
    }

    // sender path (used by any_read_sender's factory via ex::connect)
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
    auto connect(Receiver&& rcvr)
        -> op_state<std::remove_cvref_t<Receiver>>
    {
        return {std::forward<Receiver>(rcvr), pool_};
    }
};

struct noop_stream_b
{
    bench_thread_pool* pool_;

    noop_read_sender read_some(auto)
    {
        return {pool_};
    }
};

template <class Stream>
auto inner_b(
    Stream& stream,
    std::allocator_arg_t,
    std::pmr::polymorphic_allocator<std::byte>) -> io_task<>
{
    char buf[64];
    for (int i = 0; i < 10'000; ++i)
        (void)co_await stream.read_some(buf);
}

template <class Stream>
auto benchmark_b(
    bench_thread_pool* pool,
    Stream& stream,
    std::allocator_arg_t,
    std::pmr::polymorphic_allocator<std::byte> alloc) -> io_task<>
{
    auto before = g_alloc_count.load(std::memory_order_relaxed);
    auto start = std::chrono::steady_clock::now();

    for (int i = 0; i < 10'000; ++i)
        co_await inner_b(stream, std::allocator_arg, alloc);

    auto elapsed = std::chrono::steady_clock::now() - start;
    auto after = g_alloc_count.load(std::memory_order_relaxed);
    auto us = std::chrono::duration_cast<
        std::chrono::microseconds>(elapsed).count();
    auto allocs = after - before;

    std::printf(
        "B  sender monomorphized   %10lld us  %5.1f ns/op  "
        "%lld allocs (%4.2f/op)\n",
        static_cast<long long>(us),
        static_cast<double>(us) * 1000.0 / 100'000'000.0,
        static_cast<long long>(allocs),
        static_cast<double>(allocs) / 100'000'000.0);
}

// ===================================================================
// Column C — Sender + type-erased (any_read_sender)
//
// Virtual base returns any_read_sender which wraps a concrete sender
// in an inline buffer. When consumed, a factory function calls
// ex::connect through a type-erased receiver (callback_receiver)
// and heap-allocates the resulting operation state. This is the
// structural cost of type-erasing a sender: connect(sender, receiver)
// produces op_state<S, R> whose type depends on both arguments, so
// type erasure forces a heap allocation.
// ===================================================================

class any_read_sender
{
    // receiver with fixed type used inside the factory
    struct callback_receiver
    {
        using receiver_concept = ex::receiver_t;

        void* data_;
        void (*on_value_)(void*, std::size_t) noexcept;
        void (*on_stopped_)(void*) noexcept;

        struct env_t {};
        auto get_env() const noexcept -> env_t { return {}; }

        void set_value(std::size_t n) && noexcept
        {
            on_value_(data_, n);
        }

        void set_stopped() && noexcept
        {
            on_stopped_(data_);
        }

        template <class E>
        void set_error(E&&) && noexcept
        {
            std::terminate();
        }
    };

    // type-erased operation state base
    struct op_base
    {
        virtual void start() noexcept = 0;
        virtual ~op_base() = default;
    };

    // factory: takes sender buffer + callback_receiver, heap-allocates
    // a concrete op_state from ex::connect(sender, callback_receiver)
    using factory_fn = std::unique_ptr<op_base>(*)(
        void* sender_buf, callback_receiver cr);
    using destroy_fn = void(*)(void* sender_buf) noexcept;

    static constexpr std::size_t buf_size = 64;
    alignas(std::max_align_t) char buf_[buf_size];
    factory_fn factory_;
    destroy_fn destroy_;

public:
    // satisfies beman::execution::sender concept so io_task's
    // await_transform accepts it; as_awaitable takes priority
    using sender_concept = ex::sender_t;
    using completion_signatures =
        ex::completion_signatures<ex::set_value_t(std::size_t)>;

    template <class Sender>
    explicit any_read_sender(Sender s)
    {
        static_assert(sizeof(Sender) <= buf_size);
        static_assert(alignof(Sender) <= alignof(std::max_align_t));
        new (buf_) Sender(std::move(s));

        factory_ = +[](void* stor,
            callback_receiver r) -> std::unique_ptr<op_base>
        {
            auto& sndr = *static_cast<Sender*>(stor);

            using inner_op_t = decltype(ex::connect(
                std::declval<Sender>(),
                std::declval<callback_receiver>()));

            struct concrete_op : op_base
            {
                inner_op_t inner_;
                concrete_op(Sender s, callback_receiver r)
                    : inner_(ex::connect(
                        std::move(s), std::move(r))) {}
                void start() noexcept override
                {
                    ex::start(inner_);
                }
            };

            return std::make_unique<concrete_op>(
                std::move(sndr), std::move(r));
        };

        destroy_ = +[](void* stor) noexcept {
            static_cast<Sender*>(stor)->~Sender();
        };
    }

    ~any_read_sender() { destroy_(buf_); }

    any_read_sender(any_read_sender const&) = delete;
    any_read_sender& operator=(any_read_sender const&) = delete;

    any_read_sender(any_read_sender&& o) noexcept
        : factory_(o.factory_), destroy_(o.destroy_)
    {
        std::memcpy(buf_, o.buf_, buf_size);
        o.destroy_ = +[](void*) noexcept {};
    }

    any_read_sender& operator=(any_read_sender&&) = delete;

    // as_awaitable: moves the sender data into the awaitable, then
    // on await_suspend calls the factory to heap-allocate the
    // concrete op_state (the one per-operation allocation)
    template <typename Promise>
    auto as_awaitable(Promise&)
    {
        struct aw
        {
            alignas(std::max_align_t) char buf_[buf_size];
            factory_fn factory_;
            destroy_fn destroy_;
            std::unique_ptr<op_base> inner_;
            std::coroutine_handle<> cont_{};
            std::size_t result_{};

            explicit aw(any_read_sender& sndr)
                : factory_(sndr.factory_)
                , destroy_(sndr.destroy_)
            {
                std::memcpy(buf_, sndr.buf_, buf_size);
                sndr.destroy_ = +[](void*) noexcept {};
            }

            ~aw() { destroy_(buf_); }

            aw(aw const&) = delete;
            aw(aw&&) = delete;
            aw& operator=(aw const&) = delete;
            aw& operator=(aw&&) = delete;

            bool await_ready() const noexcept { return false; }

            // The factory heap-allocates via make_unique — this is the
            // per-operation allocation that the benchmark measures.
            //
            // After the callback resumes the coroutine, the coroutine
            // may destroy this awaitable (and its inner_ unique_ptr)
            // while the callback's caller is still on the stack. This
            // is the same lifecycle pattern used by Asio, folly, and
            // stdexec: the return path doesn't access freed members.
            void await_suspend(
                std::coroutine_handle<> h) noexcept
            {
                cont_ = h;
                inner_ = factory_(buf_, callback_receiver{
                    this,
                    +[](void* p, std::size_t n) noexcept {
                        auto* a = static_cast<aw*>(p);
                        a->result_ = n;
                        a->cont_.resume();
                    },
                    +[](void* p) noexcept {
                        auto* a = static_cast<aw*>(p);
                        a->cont_.resume();
                    }
                });
                inner_->start();
            }

            std::size_t await_resume() noexcept { return result_; }
        };
        return aw{*this};
    }
};

struct sender_stream_base
{
    virtual any_read_sender read_some(capy::mutable_buffer) = 0;
    virtual ~sender_stream_base() = default;
};

struct noop_stream_c : sender_stream_base
{
    bench_thread_pool* pool_;

    explicit noop_stream_c(bench_thread_pool* p) noexcept : pool_(p) {}

    any_read_sender read_some(capy::mutable_buffer) override
    {
        return any_read_sender{noop_read_sender{pool_}};
    }
};

auto inner_c(
    sender_stream_base& stream,
    std::allocator_arg_t,
    std::pmr::polymorphic_allocator<std::byte>) -> io_task<>
{
    char buf[64];
    for (int i = 0; i < 10'000; ++i)
        (void)co_await stream.read_some(
            capy::mutable_buffer(buf, sizeof(buf)));
}

auto benchmark_c(
    bench_thread_pool* pool,
    sender_stream_base& stream,
    std::allocator_arg_t,
    std::pmr::polymorphic_allocator<std::byte> alloc) -> io_task<>
{
    auto before = g_alloc_count.load(std::memory_order_relaxed);
    auto start = std::chrono::steady_clock::now();

    for (int i = 0; i < 10'000; ++i)
        co_await inner_c(stream, std::allocator_arg, alloc);

    auto elapsed = std::chrono::steady_clock::now() - start;
    auto after = g_alloc_count.load(std::memory_order_relaxed);
    auto us = std::chrono::duration_cast<
        std::chrono::microseconds>(elapsed).count();
    auto allocs = after - before;

    std::printf(
        "C  sender type-erased     %10lld us  %5.1f ns/op  "
        "%lld allocs (%4.2f/op)\n",
        static_cast<long long>(us),
        static_cast<double>(us) * 1000.0 / 100'000'000.0,
        static_cast<long long>(allocs),
        static_cast<double>(allocs) / 100'000'000.0);
}

// ===================================================================
// main
// ===================================================================

int main()
{
    std::printf(
        "type erasure benchmark: "
        "100,000,000 read_some calls per column\n\n");

    std::printf(
        "   %-24s %12s %10s  %s\n",
        "description", "time", "ns/op", "allocations");
    std::printf(
        "   %-24s %12s %10s  %s\n",
        "------------------------", "----------",
        "----------", "-------------------");

    // Column A — awaitable + virtual stream
    {
        bench_thread_pool bpool(1);
        capy::thread_pool pool(1);
        capy::run_async(pool.get_executor())(benchmark_a(bpool));
        pool.join();
        bpool.join();
    }

    // Column B — sender, monomorphized
    {
        bench_thread_pool pool(1);
        noop_stream_b stream{&pool};
        pool_scheduler sched{sender_executor{&pool}};
        auto* mr = capy::get_recycling_memory_resource();
        ex::sync_wait(ex::starts_on(sched,
            benchmark_b(
                &pool, stream,
                std::allocator_arg,
                std::pmr::polymorphic_allocator<std::byte>(mr))));
        pool.join();
    }

    // Column C — sender, type-erased
    {
        bench_thread_pool pool(1);
        noop_stream_c stream{&pool};
        pool_scheduler sched{sender_executor{&pool}};
        auto* mr = capy::get_recycling_memory_resource();
        ex::sync_wait(ex::starts_on(sched,
            benchmark_c(
                &pool, stream,
                std::allocator_arg,
                std::pmr::polymorphic_allocator<std::byte>(mr))));
        pool.join();
    }

    return 0;
}

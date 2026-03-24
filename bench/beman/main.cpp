//
// Copyright (c) 2026 Steve Gerbino
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//
// Official repository: https://github.com/cppalliance/capy
//

//
// I/O Read Stream Benchmark
//
// Compares three execution models across three stream abstraction
// levels. 100M read_some calls per cell, single thread.
//
// Table 1: capy::task        (capy::thread_pool)
// Table 2: bex::task/bex::task<void, io_env> (sender_thread_pool)
// Table 3: sender pipeline   (connect/start)
//
// Each table has three rows:
//   Native      — concrete stream, full visibility
//   Abstract    — virtual dispatch, implementation hidden
//   Type erased — value-type erasure
//

#include "allocation_tracker.hpp"
#include "awaitable_sender.hpp"
#include "ioaw_read_stream.hpp"
#include "ioaw_io_read_stream.hpp"
#include "repeat_effect_until.hpp"
#include "sender_awaitable.hpp"
#include "sndr_any_read_sender.hpp"
#include "sndr_any_read_stream.hpp"
#include "sndr_io_read_stream.hpp"
#include "sndr_read_stream.hpp"
#include "sender_io_env.hpp"

#include <boost/capy.hpp>
#include <boost/capy/io/any_read_stream.hpp>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <latch>
#include <memory>

namespace bex = beman::execution;
namespace capy = boost::capy;

// ===================================================================
// result collection
// ===================================================================

struct cell_result
{
    long long us = 0;
    int64_t allocs = 0;
};

static constexpr int kOps = 100'000'000;
static constexpr int kOuter = 10'000;
static constexpr int kInner = 10'000;

// ===================================================================
// Table 1: capy::task
//
// Templated session/accept coroutines instantiated with each
// stream type. The executor comes from io_env via capy::task's
// transform_awaiter.
// ===================================================================

template <class Stream>
capy::task<> capy_session(Stream& stream)
{
    char buf[64];
    for (int i = 0; i < kInner; ++i)
        (void)co_await stream.read_some(
            capy::mutable_buffer(buf, sizeof(buf)));
}

template <class Stream>
capy::task<> capy_accept(Stream& stream, cell_result& out)
{
    auto before = g_alloc_count.load(std::memory_order_relaxed);
    auto start = std::chrono::steady_clock::now();

    for (int i = 0; i < kOuter; ++i)
        co_await capy_session(stream);

    auto elapsed = std::chrono::steady_clock::now() - start;
    auto after = g_alloc_count.load(std::memory_order_relaxed);
    out = {std::chrono::duration_cast<
        std::chrono::microseconds>(elapsed).count(),
        after - before};
}

// ===================================================================
// Table 1: capy::task — Column B (sender via await_sender bridge)
//
// The stream returns a sender. capy::task consumes it by wrapping
// in await_sender which bridges the sender to an IoAwaitable.
// Single pool: sender_thread_pool with sender_as_capy_executor
// adapter so capy::task can run on it.
// ===================================================================

template <class Stream>
capy::task<> capy_session_sndr(Stream& stream)
{
    char buf[64];
    for (int i = 0; i < kInner; ++i)
        (void)co_await capy::await_sender(
            stream.read_some(
                capy::mutable_buffer(buf, sizeof(buf))));
}

template <class Stream>
capy::task<> capy_accept_sndr(Stream& stream, cell_result& out)
{
    auto before = g_alloc_count.load(std::memory_order_relaxed);
    auto start = std::chrono::steady_clock::now();

    for (int i = 0; i < kOuter; ++i)
        co_await capy_session_sndr(stream);

    auto elapsed = std::chrono::steady_clock::now() - start;
    auto after = g_alloc_count.load(std::memory_order_relaxed);
    out = {std::chrono::duration_cast<
        std::chrono::microseconds>(elapsed).count(),
        after - before};
}

// ===================================================================
// Table 2: bex::task — Column A (sender, native)
//
// Same templated pattern but using bex::task<void, io_env> coroutines on
// sender_thread_pool.
// ===================================================================

template <class Stream>
auto bex_session(
    Stream& stream,
    std::allocator_arg_t,
    std::pmr::polymorphic_allocator<std::byte>) -> bex::task<void, io_env>
{
    char buf[64];
    for (int i = 0; i < kInner; ++i)
        (void)co_await stream.read_some(
            capy::mutable_buffer(buf, sizeof(buf)));
}

template <class Stream>
auto bex_accept(
    sender_thread_pool* pool,
    Stream& stream,
    cell_result& out,
    std::allocator_arg_t,
    std::pmr::polymorphic_allocator<std::byte> alloc) -> bex::task<void, io_env>
{
    auto before = g_alloc_count.load(std::memory_order_relaxed);
    auto start = std::chrono::steady_clock::now();

    for (int i = 0; i < kOuter; ++i)
        co_await bex_session(stream, std::allocator_arg, alloc);

    auto elapsed = std::chrono::steady_clock::now() - start;
    auto after = g_alloc_count.load(std::memory_order_relaxed);
    out = {std::chrono::duration_cast<
        std::chrono::microseconds>(elapsed).count(),
        after - before};
}

// ===================================================================
// Table 2: bex::task — Column B (awaitable via as_sender bridge)
//
// The stream returns an IoAwaitable. bex::task consumes it by
// wrapping in as_sender which bridges the awaitable to a sender.
// ===================================================================

template <class Stream>
auto bex_session_ioaw(
    Stream& stream,
    std::allocator_arg_t,
    std::pmr::polymorphic_allocator<std::byte>) -> bex::task<void, io_env>
{
    char buf[64];
    for (int i = 0; i < kInner; ++i)
        (void)co_await capy::as_sender(
            stream.read_some(
                capy::mutable_buffer(buf, sizeof(buf))));
}

template <class Stream>
auto bex_accept_ioaw(
    sender_thread_pool* pool,
    Stream& stream,
    cell_result& out,
    std::allocator_arg_t,
    std::pmr::polymorphic_allocator<std::byte> alloc) -> bex::task<void, io_env>
{
    auto before = g_alloc_count.load(std::memory_order_relaxed);
    auto start = std::chrono::steady_clock::now();

    for (int i = 0; i < kOuter; ++i)
        co_await bex_session_ioaw(stream, std::allocator_arg, alloc);

    auto elapsed = std::chrono::steady_clock::now() - start;
    auto after = g_alloc_count.load(std::memory_order_relaxed);
    out = {std::chrono::duration_cast<
        std::chrono::microseconds>(elapsed).count(),
        after - before};
}

// ===================================================================
// main
// ===================================================================

int main()
{
    // grid[table][stream_type][column]
    // table:  0=capy::task, 1=bex::task, 2=pipeline
    // stream: 0=native, 1=abstract, 2=type_erased
    // column: 0=A (native), 1=B (bridged)
    cell_result grid[3][3][2]{};

    // ---------------------------------------------------------------
    // Table 1: capy::task
    // ---------------------------------------------------------------

    // Native — ioaw_read_stream
    {
        capy::thread_pool pool(1);
        ioaw_read_stream stream;
        capy::run_async(pool.get_executor())(
            capy_accept(stream, grid[0][0][0]));
        pool.join();
    }

    // Abstract — ioaw_io_read_stream
    {
        capy::thread_pool pool(1);
        ioaw_io_read_stream_impl stream;
        capy::run_async(pool.get_executor())(
            capy_accept(static_cast<ioaw_io_read_stream&>(stream),
                grid[0][1][0]));
        pool.join();
    }

    // Type erased — capy::any_read_stream
    {
        capy::thread_pool pool(1);
        ioaw_read_stream concrete;
        capy::any_read_stream stream(&concrete);
        capy::run_async(pool.get_executor())(
            capy_accept(stream, grid[0][2][0]));
        pool.join();
    }

    // Col B: Sender (via await_sender bridge)

    // Native — sndr_read_stream
    {
        sender_thread_pool pool(1);
        auto ex = pool.get_executor();
        bench_context ctx;
        sender_as_capy_executor adapter{ex, &ctx};
        sndr_read_stream stream{ex};
        capy::run_async(adapter)(
            capy_accept_sndr(stream, grid[0][0][1]));
        pool.join();
    }

    // Abstract — sndr_io_read_stream
    {
        sender_thread_pool pool(1);
        auto ex = pool.get_executor();
        bench_context ctx;
        sender_as_capy_executor adapter{ex, &ctx};
        sndr_io_read_stream_impl stream{ex};
        capy::run_async(adapter)(
            capy_accept_sndr(
                static_cast<sndr_io_read_stream&>(stream),
                grid[0][1][1]));
        pool.join();
    }

    // Type erased — sndr_any_read_stream
    {
        sender_thread_pool pool(1);
        auto ex = pool.get_executor();
        bench_context ctx;
        sender_as_capy_executor adapter{ex, &ctx};
        sndr_any_read_stream stream(sndr_read_stream{ex});
        capy::run_async(adapter)(
            capy_accept_sndr(stream, grid[0][2][1]));
        pool.join();
    }

    // ---------------------------------------------------------------
    // Table 2: beman::execution::task (bex::task<void, io_env>)
    // ---------------------------------------------------------------

    // Native — sndr_read_stream
    {
        sender_thread_pool pool(1);
        auto ex = pool.get_executor();
        sndr_read_stream stream{ex};
        pool_scheduler sched{ex};
        auto* mr = capy::get_recycling_memory_resource();
        bex::sync_wait(bex::starts_on(sched,
            bex_accept(
                &pool, stream, grid[1][0][0],
                std::allocator_arg,
                std::pmr::polymorphic_allocator<std::byte>(mr))));
        pool.join();
    }

    // Abstract — sndr_io_read_stream
    {
        sender_thread_pool pool(1);
        auto ex = pool.get_executor();
        sndr_io_read_stream_impl stream{ex};
        pool_scheduler sched{ex};
        auto* mr = capy::get_recycling_memory_resource();
        bex::sync_wait(bex::starts_on(sched,
            bex_accept(
                &pool,
                static_cast<sndr_io_read_stream&>(stream),
                grid[1][1][0],
                std::allocator_arg,
                std::pmr::polymorphic_allocator<std::byte>(mr))));
        pool.join();
    }

    // Type erased — sndr_any_read_stream
    {
        sender_thread_pool pool(1);
        auto ex = pool.get_executor();
        sndr_any_read_stream stream(sndr_read_stream{ex});
        pool_scheduler sched{ex};
        auto* mr = capy::get_recycling_memory_resource();
        bex::sync_wait(bex::starts_on(sched,
            bex_accept(
                &pool, stream, grid[1][2][0],
                std::allocator_arg,
                std::pmr::polymorphic_allocator<std::byte>(mr))));
        pool.join();
    }

    // Col B: Awaitable (via as_sender bridge)

    // Native — ioaw_read_stream
    {
        sender_thread_pool pool(1);
        auto ex = pool.get_executor();
        ioaw_read_stream stream;
        pool_scheduler sched{ex};
        auto* mr = capy::get_recycling_memory_resource();
        bex::sync_wait(bex::starts_on(sched,
            bex_accept_ioaw(
                &pool, stream, grid[1][0][1],
                std::allocator_arg,
                std::pmr::polymorphic_allocator<std::byte>(mr))));
        pool.join();
    }

    // Abstract — ioaw_io_read_stream
    {
        sender_thread_pool pool(1);
        auto ex = pool.get_executor();
        ioaw_io_read_stream_impl stream;
        pool_scheduler sched{ex};
        auto* mr = capy::get_recycling_memory_resource();
        bex::sync_wait(bex::starts_on(sched,
            bex_accept_ioaw(
                &pool,
                static_cast<ioaw_io_read_stream&>(stream),
                grid[1][1][1],
                std::allocator_arg,
                std::pmr::polymorphic_allocator<std::byte>(mr))));
        pool.join();
    }

    // Type erased — capy::any_read_stream
    {
        sender_thread_pool pool(1);
        auto ex = pool.get_executor();
        ioaw_read_stream concrete;
        capy::any_read_stream stream(&concrete);
        pool_scheduler sched{ex};
        auto* mr = capy::get_recycling_memory_resource();
        bex::sync_wait(bex::starts_on(sched,
            bex_accept_ioaw(
                &pool, stream, grid[1][2][1],
                std::allocator_arg,
                std::pmr::polymorphic_allocator<std::byte>(mr))));
        pool.join();
    }

    // ---------------------------------------------------------------
    // Table 3: sender/receiver pipeline (repeat_effect_until)
    // ---------------------------------------------------------------

    // Col A: Sender (native)

    // Native — sndr_read_stream
    {
        sender_thread_pool pool(1);
        auto ex = pool.get_executor();
        sndr_read_stream stream{ex};
        pool_scheduler sched{ex};
        int count = kOps;
        char buf[64];
        auto before = g_alloc_count.load(
            std::memory_order_relaxed);
        auto start = std::chrono::steady_clock::now();
        bex::sync_wait(bex::starts_on(sched,
            repeat_effect_until(
                bex::let_value(bex::just(), [&]() {
                    return stream.read_some(
                        capy::mutable_buffer(buf, sizeof(buf)));
                }),
                [&count]() { return --count == 0; })));
        pool.join();
        auto elapsed =
            std::chrono::steady_clock::now() - start;
        auto after = g_alloc_count.load(
            std::memory_order_relaxed);
        grid[2][0][0] = {
            std::chrono::duration_cast<
                std::chrono::microseconds>(elapsed).count(),
            after - before};
    }

    // Abstract — sndr_io_read_stream
    {
        sender_thread_pool pool(1);
        auto ex = pool.get_executor();
        sndr_io_read_stream_impl stream{ex};
        pool_scheduler sched{ex};
        int count = kOps;
        char buf[64];
        auto before = g_alloc_count.load(
            std::memory_order_relaxed);
        auto start = std::chrono::steady_clock::now();
        bex::sync_wait(bex::starts_on(sched,
            repeat_effect_until(
                bex::let_value(bex::just(), [&]() {
                    return static_cast<sndr_io_read_stream&>(
                        stream).read_some(
                            capy::mutable_buffer(buf, sizeof(buf)));
                }),
                [&count]() { return --count == 0; })));
        pool.join();
        auto elapsed =
            std::chrono::steady_clock::now() - start;
        auto after = g_alloc_count.load(
            std::memory_order_relaxed);
        grid[2][1][0] = {
            std::chrono::duration_cast<
                std::chrono::microseconds>(elapsed).count(),
            after - before};
    }

    // Type erased — sndr_any_read_stream
    {
        sender_thread_pool pool(1);
        auto ex = pool.get_executor();
        sndr_any_read_stream stream(sndr_read_stream{ex});
        pool_scheduler sched{ex};
        int count = kOps;
        char buf[64];
        auto before = g_alloc_count.load(
            std::memory_order_relaxed);
        auto start = std::chrono::steady_clock::now();
        bex::sync_wait(bex::starts_on(sched,
            repeat_effect_until(
                bex::let_value(bex::just(), [&]() {
                    return stream.read_some(
                        capy::mutable_buffer(buf, sizeof(buf)));
                }),
                [&count]() { return --count == 0; })));
        pool.join();
        auto elapsed =
            std::chrono::steady_clock::now() - start;
        auto after = g_alloc_count.load(
            std::memory_order_relaxed);
        grid[2][2][0] = {
            std::chrono::duration_cast<
                std::chrono::microseconds>(elapsed).count(),
            after - before};
    }

    // Col B: Awaitable (via as_sender bridge)

    // Native — ioaw_read_stream
    {
        sender_thread_pool pool(1);
        ioaw_read_stream stream;
        pool_scheduler sched{pool.get_executor()};
        int count = kOps;
        char buf[64];
        auto before = g_alloc_count.load(
            std::memory_order_relaxed);
        auto start = std::chrono::steady_clock::now();
        bex::sync_wait(bex::starts_on(sched,
            repeat_effect_until(
                bex::let_value(bex::just(), [&]() {
                    return capy::as_sender(stream.read_some(
                        capy::mutable_buffer(buf, sizeof(buf))));
                }),
                [&count]() { return --count == 0; })));
        pool.join();
        auto elapsed =
            std::chrono::steady_clock::now() - start;
        auto after = g_alloc_count.load(
            std::memory_order_relaxed);
        grid[2][0][1] = {
            std::chrono::duration_cast<
                std::chrono::microseconds>(elapsed).count(),
            after - before};
    }

    // Abstract — ioaw_io_read_stream
    {
        sender_thread_pool pool(1);
        ioaw_io_read_stream_impl stream;
        pool_scheduler sched{pool.get_executor()};
        int count = kOps;
        char buf[64];
        auto before = g_alloc_count.load(
            std::memory_order_relaxed);
        auto start = std::chrono::steady_clock::now();
        bex::sync_wait(bex::starts_on(sched,
            repeat_effect_until(
                bex::let_value(bex::just(), [&]() {
                    return capy::as_sender(
                        static_cast<ioaw_io_read_stream&>(
                            stream).read_some(
                                capy::mutable_buffer(
                                    buf, sizeof(buf))));
                }),
                [&count]() { return --count == 0; })));
        pool.join();
        auto elapsed =
            std::chrono::steady_clock::now() - start;
        auto after = g_alloc_count.load(
            std::memory_order_relaxed);
        grid[2][1][1] = {
            std::chrono::duration_cast<
                std::chrono::microseconds>(elapsed).count(),
            after - before};
    }

    // Type erased — capy::any_read_stream
    {
        sender_thread_pool pool(1);
        ioaw_read_stream concrete;
        capy::any_read_stream stream(&concrete);
        pool_scheduler sched{pool.get_executor()};
        int count = kOps;
        char buf[64];
        auto before = g_alloc_count.load(
            std::memory_order_relaxed);
        auto start = std::chrono::steady_clock::now();
        bex::sync_wait(bex::starts_on(sched,
            repeat_effect_until(
                bex::let_value(bex::just(), [&]() {
                    return capy::as_sender(stream.read_some(
                        capy::mutable_buffer(buf, sizeof(buf))));
                }),
                [&count]() { return --count == 0; })));
        pool.join();
        auto elapsed =
            std::chrono::steady_clock::now() - start;
        auto after = g_alloc_count.load(
            std::memory_order_relaxed);
        grid[2][2][1] = {
            std::chrono::duration_cast<
                std::chrono::microseconds>(elapsed).count(),
            after - before};
    }

    // ---------------------------------------------------------------
    // Print results
    // ---------------------------------------------------------------

    constexpr double ops = static_cast<double>(kOps);

    std::printf(
        "I/O read stream benchmark: "
        "%d read_some calls per cell\n", kOps);

    auto print_table = [&](
        char const* title,
        int table,
        char const* col_a_label,
        char const* col_b_label,
        char const* labels[3],
        bool has_col_b)
    {
        std::printf("\n  %s\n", title);
        if (has_col_b)
        {
            std::printf(
                "  %-24s  %-26s  %-26s\n",
                "", col_a_label, col_b_label);
            std::printf(
                "  %-24s  %-26s  %-26s\n",
                "------------------------",
                "--------------------------",
                "--------------------------");
        }
        else
        {
            std::printf(
                "  %-24s  %-26s\n",
                "", col_a_label);
            std::printf(
                "  %-24s  %-26s\n",
                "------------------------",
                "--------------------------");
        }

        auto alloc_per_op = [](int64_t allocs) -> double {
            return static_cast<double>(allocs) / ops;
        };

        for (int s = 0; s < 3; ++s)
        {
            auto& a = grid[table][s][0];
            if (has_col_b)
            {
                auto& b = grid[table][s][1];
                std::printf(
                    "  %-24s  %6.1f ns/op  %3.0f al/op"
                    "    %6.1f ns/op  %3.0f al/op\n",
                    labels[s],
                    static_cast<double>(a.us) * 1000.0 / ops,
                    alloc_per_op(a.allocs),
                    static_cast<double>(b.us) * 1000.0 / ops,
                    alloc_per_op(b.allocs));
            }
            else
            {
                std::printf(
                    "  %-24s  %6.1f ns/op  %3.0f al/op\n",
                    labels[s],
                    static_cast<double>(a.us) * 1000.0 / ops,
                    alloc_per_op(a.allocs));
            }
        }
    };

    char const* ioaw_labels[] = {
        "ioaw_read_stream",
        "ioaw_io_read_stream",
        "capy::any_read_stream"};
    char const* sndr_labels[] = {
        "sndr_read_stream",
        "sndr_io_read_stream",
        "sndr_any_read_stream"};

    print_table(
        "capy::task",
        0,
        "A: awaitable",
        "B: sender (bridge)",
        ioaw_labels,
        true);

    print_table(
        "beman::execution::task",
        1,
        "A: sender (native)",
        "B: awaitable (bridge)",
        sndr_labels,
        true);

    print_table(
        "sender/receiver pipeline",
        2,
        "A: sender (native)",
        "B: awaitable (bridge)",
        sndr_labels,
        true);

    return 0;
}

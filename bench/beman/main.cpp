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
#include "ioaw_read_stream.hpp"
#include "ioaw_io_read_stream.hpp"
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
capy::task<> session(Stream& stream)
{
    char buf[64];
    for (int i = 0; i < kInner; ++i)
        (void)co_await stream.read_some(
            capy::mutable_buffer(buf, sizeof(buf)));
}

template <class Stream>
capy::task<> accept(Stream& stream, cell_result& out)
{
    auto before = g_alloc_count.load(std::memory_order_relaxed);
    auto start = std::chrono::steady_clock::now();

    for (int i = 0; i < kOuter; ++i)
        co_await session(stream);

    auto elapsed = std::chrono::steady_clock::now() - start;
    auto after = g_alloc_count.load(std::memory_order_relaxed);
    out = {std::chrono::duration_cast<
        std::chrono::microseconds>(elapsed).count(),
        after - before};
}

// ===================================================================
// Table 2: beman::execution::task (bex::task<void, io_env>)
//
// Same templated pattern but using bex::task<void, io_env> coroutines on
// sender_thread_pool.
// ===================================================================

template <class Stream>
auto sndr_session(
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
auto sndr_accept(
    sender_thread_pool* pool,
    Stream& stream,
    cell_result& out,
    std::allocator_arg_t,
    std::pmr::polymorphic_allocator<std::byte> alloc) -> bex::task<void, io_env>
{
    auto before = g_alloc_count.load(std::memory_order_relaxed);
    auto start = std::chrono::steady_clock::now();

    for (int i = 0; i < kOuter; ++i)
        co_await sndr_session(stream, std::allocator_arg, alloc);

    auto elapsed = std::chrono::steady_clock::now() - start;
    auto after = g_alloc_count.load(std::memory_order_relaxed);
    out = {std::chrono::duration_cast<
        std::chrono::microseconds>(elapsed).count(),
        after - before};
}

// ===================================================================
// Table 3: sender/receiver pipeline — Native
//
// Concrete sndr_read_stream, connect/start driven by callback
// receiver. Inline op_state, zero allocation per operation.
// ===================================================================

struct native_pipeline_runner;

struct native_pipeline_receiver
{
    using receiver_concept = bex::receiver_t;
    native_pipeline_runner* runner_;

    struct env_t {};
    auto get_env() const noexcept -> env_t { return {}; }

    void set_value(std::size_t) && noexcept;
    void set_stopped() && noexcept {}

    template <class E>
    void set_error(E&&) && noexcept { std::terminate(); }
};

using native_sender_t = sndr_read_stream::read_sender;
using native_op_t = native_sender_t::op_state<native_pipeline_receiver>;

struct native_pipeline_runner
{
    sndr_read_stream& stream_;
    int remaining_;
    std::latch& done_;
    int64_t alloc_before_;
    std::chrono::steady_clock::time_point start_time_;
    cell_result result_{};

    alignas(native_op_t) char op_buf_[sizeof(native_op_t)];
    bool op_active_ = false;

    void destroy_op()
    {
        if (op_active_)
        {
            reinterpret_cast<native_op_t*>(op_buf_)->~native_op_t();
            op_active_ = false;
        }
    }

    void run_one()
    {
        if (remaining_ <= 0)
        {
            destroy_op();
            auto elapsed =
                std::chrono::steady_clock::now() - start_time_;
            result_ = {
                std::chrono::duration_cast<
                    std::chrono::microseconds>(elapsed).count(),
                g_alloc_count.load(std::memory_order_relaxed)
                    - alloc_before_};
            done_.count_down();
            return;
        }
        --remaining_;

        destroy_op();

        char buf[64];
        auto* op = ::new (op_buf_) native_op_t(
            stream_.read_some(buf)
            .connect(native_pipeline_receiver{this}));
        op_active_ = true;
        bex::start(*op);
    }
};

void native_pipeline_receiver::set_value(std::size_t) && noexcept
{
    runner_->run_one();
}

// ===================================================================
// Table 3: sender/receiver pipeline — Abstract / Type erased
//
// Stream returns sndr_any_read_sender. connect() heap-allocates
// the operation state via unique_ptr<op_base>. Templated on
// stream type so it works with both sndr_io_read_stream (abstract)
// and sndr_any_read_stream (value-type erased).
// ===================================================================

template <class Stream>
struct pipeline_runner
{
    Stream& stream_;
    int remaining_;
    std::latch& done_;
    int64_t alloc_before_;
    std::chrono::steady_clock::time_point start_time_;
    cell_result result_{};

    std::unique_ptr<sndr_any_read_sender::op_base> op_;

    void run_one()
    {
        if (remaining_ <= 0)
        {
            op_.reset();
            auto elapsed =
                std::chrono::steady_clock::now() - start_time_;
            result_ = {
                std::chrono::duration_cast<
                    std::chrono::microseconds>(elapsed).count(),
                g_alloc_count.load(std::memory_order_relaxed)
                    - alloc_before_};
            done_.count_down();
            return;
        }
        --remaining_;

        op_.reset();

        char buf[64];
        auto sender = stream_.read_some(
            capy::mutable_buffer(buf, sizeof(buf)));
        op_ = sender.connect(
            this,
            +[](void* p, std::size_t) noexcept {
                static_cast<pipeline_runner*>(p)->run_one();
            },
            +[](void*) noexcept {});
        op_->start();
    }
};

// ===================================================================
// main
// ===================================================================

int main()
{
    // grid[table][stream_type]
    // table: 0=capy::task, 1=bex::task<void, io_env>, 2=pipeline
    // stream: 0=native, 1=abstract, 2=type_erased
    cell_result grid[3][3]{};

    // ---------------------------------------------------------------
    // Table 1: capy::task
    // ---------------------------------------------------------------

    // Native — ioaw_read_stream
    {
        capy::thread_pool pool(1);
        ioaw_read_stream stream;
        capy::run_async(pool.get_executor())(
            accept(stream, grid[0][0]));
        pool.join();
    }

    // Abstract — ioaw_io_read_stream
    {
        capy::thread_pool pool(1);
        ioaw_io_read_stream_impl stream;
        capy::run_async(pool.get_executor())(
            accept(static_cast<ioaw_io_read_stream&>(stream),
                grid[0][1]));
        pool.join();
    }

    // Type erased — capy::any_read_stream
    {
        capy::thread_pool pool(1);
        ioaw_read_stream concrete;
        capy::any_read_stream stream(&concrete);
        capy::run_async(pool.get_executor())(
            accept(stream, grid[0][2]));
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
            sndr_accept(
                &pool, stream, grid[1][0],
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
            sndr_accept(
                &pool,
                static_cast<sndr_io_read_stream&>(stream),
                grid[1][1],
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
            sndr_accept(
                &pool, stream, grid[1][2],
                std::allocator_arg,
                std::pmr::polymorphic_allocator<std::byte>(mr))));
        pool.join();
    }

    // ---------------------------------------------------------------
    // Table 3: sender/receiver pipeline
    // ---------------------------------------------------------------

    // Native — sndr_read_stream
    {
        sender_thread_pool pool(1);
        sndr_read_stream stream{pool.get_executor()};
        std::latch done(1);
        native_pipeline_runner runner{
            stream, kOps, done,
            g_alloc_count.load(std::memory_order_relaxed),
            std::chrono::steady_clock::now()};
        runner.run_one();
        done.wait();
        pool.join();
        grid[2][0] = runner.result_;
    }

    // Abstract — sndr_io_read_stream
    {
        sender_thread_pool pool(1);
        auto ex = pool.get_executor();
        sndr_io_read_stream_impl stream{ex};
        std::latch done(1);
        pipeline_runner<sndr_io_read_stream> runner{
            stream, kOps, done,
            g_alloc_count.load(std::memory_order_relaxed),
            std::chrono::steady_clock::now()};
        runner.run_one();
        done.wait();
        pool.join();
        grid[2][1] = runner.result_;
    }

    // Type erased — sndr_any_read_stream
    {
        sender_thread_pool pool(1);
        auto ex = pool.get_executor();
        sndr_any_read_stream stream(sndr_read_stream{ex});
        std::latch done(1);
        pipeline_runner<sndr_any_read_stream> runner{
            stream, kOps, done,
            g_alloc_count.load(std::memory_order_relaxed),
            std::chrono::steady_clock::now()};
        runner.run_one();
        done.wait();
        pool.join();
        grid[2][2] = runner.result_;
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
        char const* native_label,
        char const* abstract_label,
        char const* erased_label)
    {
        std::printf("\n  %s\n", title);
        std::printf(
            "  %-30s  %10s  %10s\n",
            "", "ns/op", "allocs");
        std::printf(
            "  %-30s  %10s  %10s\n",
            "------------------------------",
            "----------",
            "----------");

        auto row = [&](char const* label, int stream) {
            auto& c = grid[table][stream];
            std::printf(
                "  %-30s  %8.1f    %8lld\n",
                label,
                static_cast<double>(c.us) * 1000.0 / ops,
                static_cast<long long>(c.allocs));
        };

        row(native_label, 0);
        row(abstract_label, 1);
        row(erased_label, 2);
    };

    print_table(
        "capy::task",
        0,
        "ioaw_read_stream",
        "ioaw_io_read_stream",
        "capy::any_read_stream");

    print_table(
        "beman::execution::task",
        1,
        "sndr_read_stream",
        "sndr_io_read_stream",
        "sndr_any_read_stream");

    print_table(
        "sender/receiver pipeline",
        2,
        "sndr_read_stream",
        "sndr_io_read_stream",
        "sndr_any_read_stream");

    return 0;
}

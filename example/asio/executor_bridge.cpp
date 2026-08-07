//
// Copyright (c) 2026 Steve Gerbino
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//
// Official repository: https://github.com/cppalliance/capy
//

#include "api/asio_executor.hpp"

#include <boost/asio/basic_socket_acceptor.hpp>
#include <boost/asio/basic_stream_socket.hpp>
#include <boost/asio/basic_waitable_timer.hpp>
#include <boost/asio/buffer.hpp>
#include <boost/asio/defer.hpp>
#include <boost/asio/dispatch.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/prefer.hpp>
#include <boost/asio/query.hpp>
#include <boost/asio/read.hpp>
#include <boost/asio/require.hpp>
#include <boost/asio/strand.hpp>
#include <boost/asio/write.hpp>

#include <boost/capy/ex/thread_pool.hpp>
#include <boost/capy/ex/async_waker.hpp>
#include <boost/capy/ex/run_async.hpp>
#include <boost/capy/task.hpp>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <latch>
#include <string>
#include <thread>
#include <vector>

// The demo must verify in NDEBUG builds, so assert() is out.
#define BRIDGE_CHECK(cond)                                      \
    do                                                          \
    {                                                           \
        if(!(cond))                                             \
        {                                                       \
            std::fprintf(stderr,                                \
                "BRIDGE_CHECK failed at %s:%d: %s\n",           \
                __FILE__, __LINE__, #cond);                     \
            std::abort();                                       \
        }                                                       \
    } while(0)

using bridge_executor = asio_executor<capy::thread_pool::executor_type>;

// I/O objects rebound to the bridge executor: services live in the
// bridge's io_context (context query), completions dispatch through
// the bridge executor onto the capy pool. No bind_executor anywhere.
using bridge_timer = net::basic_waitable_timer<
    std::chrono::steady_clock,
    net::wait_traits<std::chrono::steady_clock>,
    bridge_executor>;
using bridge_socket =
    net::basic_stream_socket<net::ip::tcp, bridge_executor>;
using bridge_acceptor =
    net::basic_socket_acceptor<net::ip::tcp, bridge_executor>;

static_assert(net::execution::is_executor<bridge_executor>::value);
static_assert(net::can_require<bridge_executor,
    net::execution::blocking_t::never_t>::value);
static_assert(net::can_query<bridge_executor,
    net::execution::context_t>::value);
static_assert(net::can_prefer<bridge_executor,
    net::execution::outstanding_work_t::tracked_t>::value);

// asio's three submission functions all land on the capy pool.
// join() returns only when the bridged nodes have finished their
// work pairing, so the count is stable when we read it.
void test_post_dispatch_defer()
{
    std::atomic<int> count{0};
    capy::thread_pool pool(4);
    {
        bridge_context ctx(pool.get_executor());
        auto ex = ctx.get_executor();

        net::post(ex, [&]{ ++count; });
        net::dispatch(ex, [&]{ ++count; });
        net::defer(ex, [&]{ ++count; });
    }
    pool.join();
    BRIDGE_CHECK(count == 3);
}

// asio's generic strand wraps the bridge executor: its service
// registers in the bridge's io_context (via the context query) and
// its invocations run serialized on the capy pool. The counter is
// deliberately unsynchronized; only strand serialization protects
// it, and join() orders the final read.
void test_strand()
{
    int counter = 0;
    capy::thread_pool pool(4);
    bridge_context ctx(pool.get_executor());
    auto strand = net::make_strand(ctx.get_executor());

    std::vector<std::thread> producers;
    for(int t = 0; t < 4; ++t)
        producers.emplace_back([&]
        {
            for(int i = 0; i < 1000; ++i)
                net::post(strand, [&]{ ++counter; });
        });
    for(auto& th : producers)
        th.join();

    // The strand's bridged invokers hold capy work until every
    // queued handler has run, so join() drains all 4000 increments
    // before ctx (and the strand service) is destroyed at scope
    // exit. Destroying ctx first would race the invoker and could
    // drop queued handlers.
    pool.join();
    BRIDGE_CHECK(counter == 4000);
}

// A tracked executor copy (what make_work_guard stores) pairs
// capy's on_work_started/on_work_finished, so pool.join() must
// block until the guard is reset. `released` is set before the
// reset, so join() returning proves it waited.
void test_work_guard()
{
    capy::thread_pool pool(2);
    std::atomic<bool> released{false};
    {
        bridge_context ctx(pool.get_executor());
        auto guard = net::make_work_guard(ctx.get_executor());

        std::thread t([&]
        {
            std::this_thread::sleep_for(
                std::chrono::milliseconds(50));
            released = true;
            guard.reset();
        });
        pool.join();
        BRIDGE_CHECK(released);
        t.join();
    }
}

// A capy coroutine and asio-posted callbacks ping-pong through the
// bridge. The pool has ONE thread: async_waker::wait() requires an
// executor that never runs the waiter's continuations concurrently,
// and the single thread also makes the log ordering deterministic
// and race-free without synchronization.
capy::task<>
ping_pong(
    bridge_executor ex,
    std::vector<std::string>& log,
    std::latch& done)
{
    capy::async_waker waker;
    for(int i = 0; i < 3; ++i)
    {
        net::post(ex, [&]
        {
            log.push_back("asio");
            waker.wake();
        });
        auto [ec] = co_await waker.wait();
        BRIDGE_CHECK(!ec);
        log.push_back("capy");
    }
    done.count_down();
}

void test_interleaving()
{
    std::vector<std::string> log;
    capy::thread_pool pool(1);
    {
        bridge_context ctx(pool.get_executor());
        std::latch done(1);
        capy::run_async(pool.get_executor())(
            ping_pong(ctx.get_executor(), log, done));
        // run_async returns immediately; the latch keeps ctx (and
        // the executor copies pointing into it) alive until the
        // coroutine finishes.
        done.wait();
    }
    pool.join();

    std::vector<std::string> const expected =
        {"asio", "capy", "asio", "capy", "asio", "capy"};
    BRIDGE_CHECK(log == expected);
}

// The reactor wait runs on the bridge's hidden pump thread, but the
// completion handler must not: running_in_this_thread() on the
// internal io_context proves the handler hopped to the capy pool.
void test_timer()
{
    std::atomic<bool> fired{false};
    capy::thread_pool pool(4);
    {
        bridge_context ctx(pool.get_executor());
        bridge_timer timer(
            ctx.get_executor(), std::chrono::milliseconds(10));

        std::latch done(1);
        timer.async_wait(
            [&](boost::system::error_code ec)
            {
                BRIDGE_CHECK(!ec);
                BRIDGE_CHECK(!ctx.io().get_executor()
                    .running_in_this_thread());
                fired = true;
                done.count_down();
            });
        done.wait();
    }
    pool.join();
    BRIDGE_CHECK(fired);
}

// Full asio socket I/O with bridge-typed objects: accept, connect,
// write, read — every completion on the capy pool. Loopback with
// port 0 follows the make_stream_pair precedent in
// api/capy_streams.cpp.
void test_socket_echo()
{
    capy::thread_pool pool(4);
    std::string received;
    {
        bridge_context ctx(pool.get_executor());
        auto ex = ctx.get_executor();

        bridge_acceptor acceptor(ex,
            net::ip::tcp::endpoint(
                net::ip::address_v4::loopback(), 0));
        bridge_socket server(ex);
        bridge_socket client(ex);
        char buf[5] = {};

        std::latch done(2);

        acceptor.async_accept(server,
            [&](boost::system::error_code ec)
            {
                BRIDGE_CHECK(!ec);
                BRIDGE_CHECK(!ctx.io().get_executor()
                    .running_in_this_thread());
                net::async_read(server, net::buffer(buf),
                    [&](boost::system::error_code ec, std::size_t n)
                    {
                        BRIDGE_CHECK(!ec);
                        BRIDGE_CHECK(n == 5);
                        received.assign(buf, 5);
                        done.count_down();
                    });
            });

        client.async_connect(acceptor.local_endpoint(),
            [&](boost::system::error_code ec)
            {
                BRIDGE_CHECK(!ec);
                net::async_write(client, net::buffer("hello", 5),
                    [&](boost::system::error_code ec, std::size_t n)
                    {
                        BRIDGE_CHECK(!ec);
                        BRIDGE_CHECK(n == 5);
                        done.count_down();
                    });
            });

        done.wait();
    }
    pool.join();
    BRIDGE_CHECK(received == "hello");
}

int main()
{
    test_post_dispatch_defer();
    test_strand();
    test_work_guard();
    test_interleaving();
    test_timer();
    test_socket_echo();
    std::printf("executor_bridge: all checks passed\n");
}

//
// Copyright (c) 2026 Steve Gerbino
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//
// Official repository: https://github.com/cppalliance/capy
//

// Test that header file is self-contained.
#include <boost/capy/ex/parking_service.hpp>

#include <boost/capy/ex/await_context.hpp>
#include <boost/capy/ex/ready_queue.hpp>
#include <boost/capy/ex/run_async.hpp>
#include <boost/capy/task.hpp>

#include <coroutine>
#include <utility>

#include "test_helpers.hpp"

namespace boost {
namespace capy {

namespace {

// Gives up what it holds when it closes, as a real context does.
class loop_context : public execution_context
{
public:
    ready_queue<> q;
    bool closed = false;

    loop_context() : execution_context(this) {}

    ~loop_context()
    {
        closed = true;
        shutdown();
        drain();
        destroy();
        drain();
    }

    void close() { shutdown(); }

    void drain()
    {
        while(auto h = q.pop())
        {
        }
    }

    void pump()
    {
        while(auto h = q.pop())
            safe_resume(h.release()->h);
    }

    class executor_type
    {
        loop_context* c_ = nullptr;
    public:
        executor_type() = default;
        explicit executor_type(loop_context& c) noexcept : c_(&c) {}
        bool operator==(executor_type const& o) const noexcept
        {
            return c_ == o.c_;
        }
        execution_context& context() const noexcept { return *c_; }
        void on_work_started() const noexcept {}
        void on_work_finished() const noexcept {}
        std::coroutine_handle<> dispatch(chain_holder&& h) const
        {
            post(std::move(h));
            return std::noop_coroutine();
        }
        void post(chain_holder&& h) const
        {
            if(!c_->closed)
                c_->q.push(std::move(h));
        }
    };

    executor_type get_executor() noexcept { return executor_type(*this); }
};

class test_service : public parking_service
{
    void on_give_up(waiter&) noexcept override { ++given_up; }

public:
    int given_up = 0;
    int live = 0;
    waiter* last = nullptr;

    void add(waiter& w)
    {
        last = &w;
        park(w);
    }

    chain_holder claim(waiter& w) noexcept { return unpark(w); }
};

struct service_a : test_service
{
    using key_type = service_a;
    explicit service_a(execution_context&) {}
};

struct service_b : test_service
{
    using key_type = service_b;
    explicit service_b(execution_context&) {}
};

// Lives in a frame and touches its service when destroyed.
class io_object
{
    test_service& s_;

public:
    explicit io_object(test_service& s) : s_(s) { ++s_.live; }
    ~io_object() { --s_.live; }
};

template<class S>
struct park_on
{
    S& s;
    parking_service::waiter w;

    bool await_ready() const noexcept { return false; }

    void await_suspend(
        std::coroutine_handle<> h, await_context const* env) noexcept
    {
        w.cont = continuation{h, env->owner};
        s.add(w);
    }

    void await_resume() const noexcept {}
};

template<class Parks, class Owns>
task<void>
owns_then_parks(Parks& parks, Owns& owns, int& down)
{
    struct count
    {
        int& n;
        ~count() { ++n; }
    } c{down};
    io_object o(owns);
    co_await park_on<Parks>{parks, {}};
}

} // namespace

struct parking_service_test
{
    void testShutdownGivesUpParked()
    {
        int down = 0;
        {
            loop_context ctx;
            auto& a = ctx.use_service<service_a>();
            auto& b = ctx.use_service<service_b>();
            run_async(ctx.get_executor())(owns_then_parks(a, b, down));
            ctx.pump();
            BOOST_TEST_EQ(down, 0);
        }
        BOOST_TEST_EQ(down, 1);
    }

    // Each chain parks in one service and owns an object of the other,
    // so no destruction order alone could be safe.
    void testCrossingChains()
    {
        int down = 0;
        {
            loop_context ctx;
            auto& a = ctx.use_service<service_a>();
            auto& b = ctx.use_service<service_b>();
            run_async(ctx.get_executor())(owns_then_parks(a, b, down));
            run_async(ctx.get_executor())(owns_then_parks(b, a, down));
            ctx.pump();
            BOOST_TEST_EQ(a.live + b.live, 2);
        }
        BOOST_TEST_EQ(down, 2);
    }

    void testHookRunsForEachGiveUp()
    {
        int down = 0;
        int given_up = 0;
        {
            loop_context ctx;
            auto& a = ctx.use_service<service_a>();
            auto& b = ctx.use_service<service_b>();
            run_async(ctx.get_executor())(owns_then_parks(a, b, down));
            run_async(ctx.get_executor())(owns_then_parks(a, b, down));
            ctx.pump();
            ctx.close();
            given_up = a.given_up;
        }
        BOOST_TEST_EQ(given_up, 2);
        BOOST_TEST_EQ(down, 2);
    }

    void testOnlyOneClaimWins()
    {
        int down = 0;
        {
            loop_context ctx;
            auto& a = ctx.use_service<service_a>();
            auto& b = ctx.use_service<service_b>();
            run_async(ctx.get_executor())(owns_then_parks(a, b, down));
            ctx.pump();

            auto& w = *a.last;
            auto first = a.claim(w);
            auto second = a.claim(w);
            BOOST_TEST(bool(first));
            BOOST_TEST(!second);
            ctx.get_executor().post(std::move(first));
            ctx.pump();
            BOOST_TEST_EQ(down, 1);
            BOOST_TEST_EQ(a.given_up, 0);
        }
        BOOST_TEST_EQ(down, 1);
    }

    void testParkAfterShutdownIsRefused()
    {
        int down = 0;
        {
            loop_context ctx;
            auto& a = ctx.use_service<service_a>();
            auto& b = ctx.use_service<service_b>();
            ctx.close();
            run_async(ctx.get_executor())(owns_then_parks(a, b, down));
            ctx.pump();
            ctx.closed = true;
            ctx.drain();
        }
        BOOST_TEST_EQ(down, 1);
    }

    void run()
    {
        testShutdownGivesUpParked();
        testCrossingChains();
        testHookRunsForEachGiveUp();
        testOnlyOneClaimWins();
        testParkAfterShutdownIsRefused();
    }
};

TEST_SUITE(
    parking_service_test,
    "boost.capy.parking_service");

} // namespace capy
} // namespace boost

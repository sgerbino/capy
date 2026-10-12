//
// Copyright (c) 2025 Vinnie Falco (vinnie.falco@gmail.com)
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//
// Official repository: https://github.com/cppalliance/capy
//

// Test that header file is self-contained.
#include <boost/capy/test/run_blocking.hpp>

#include <boost/capy/task.hpp>
#include <boost/capy/ex/strand.hpp>
#include <boost/capy/ex/this_coro.hpp>

#include "test_helpers.hpp"
#include "test_suite.hpp"

#include <optional>
#include <stdexcept>
#include <string>
#include <utility>

namespace boost {
namespace capy {
namespace test {

static_assert(Executor<blocking_executor>);
static_assert(ExecutionContext<blocking_context>);

//----------------------------------------------------------
// Test Exception Type
//----------------------------------------------------------

struct test_exception : std::runtime_error
{
    explicit test_exception(char const* msg)
        : std::runtime_error(msg)
    {
    }
};

namespace {

// Posts a continuation from its destructor, as a frame-local primitive
// does when it is torn down.
struct posts_on_destroy
{
    blocking_executor ex;
    continuation* c;

    posts_on_destroy(blocking_executor e, continuation* cc) noexcept
        : ex(e)
        , c(cc)
    {
    }

    posts_on_destroy(posts_on_destroy&& o) noexcept
        : ex(o.ex)
        , c(std::exchange(o.c, nullptr))
    {
    }

    ~posts_on_destroy()
    {
        if(c)
            ex.post(*c);
    }
};

task<void>
posting_probe(destroy_count_t, posts_on_destroy)
{
    co_return;
}

task<void>
plain_probe(destroy_count_t)
{
    co_return;
}

// Posts from its destructor, after the context's services shut down.
struct dtor_poster : execution_context::service
{
    blocking_executor ex_;
    continuation* c_;

    dtor_poster(execution_context&, blocking_executor ex, continuation& c)
        : ex_(ex)
        , c_(&c)
    {
    }

    ~dtor_poster()
    {
        ex_.post(*c_);
    }

    void shutdown() override {}
};

} // namespace

//----------------------------------------------------------
// run_blocking Tests
//----------------------------------------------------------

struct run_blocking_test
{
    //----------------------------------------------------------
    // Basic Functionality
    //----------------------------------------------------------

    static task<int>
    returns_int()
    {
        co_return 42;
    }

    static task<void>
    returns_void()
    {
        co_return;
    }

    static task<std::string>
    returns_string()
    {
        co_return "hello";
    }

    void
    testVoidTaskNoHandler()
    {
        // Should complete without blocking forever
        run_blocking()(returns_void());
        BOOST_TEST(true);
    }

    void
    testIntTaskNoHandler()
    {
        // Result is discarded
        run_blocking()(returns_int());
        BOOST_TEST(true);
    }

    void
    testIntTaskWithHandler()
    {
        int result = 0;
        run_blocking([&](int v) { result = v; })(returns_int());
        BOOST_TEST_EQ(result, 42);
    }

    void
    testVoidTaskWithHandler()
    {
        bool called = false;
        run_blocking([&]() { called = true; })(returns_void());
        BOOST_TEST(called);
    }

    void
    testStringTaskWithHandler()
    {
        std::string result;
        run_blocking([&](std::string s) { result = std::move(s); })(
            returns_string());
        BOOST_TEST_EQ(result, "hello");
    }

    void
    testDualHandlers()
    {
        int result = 0;
        bool error_called = false;

        run_blocking(
            [&](int v) { result = v; },
            [&](std::exception_ptr) { error_called = true; }
        )(returns_int());

        BOOST_TEST_EQ(result, 42);
        BOOST_TEST(!error_called);
    }

    //----------------------------------------------------------
    // Exception Handling
    //----------------------------------------------------------

    static task<int>
    throws_exception()
    {
        throw test_exception("test error");
        co_return 0;
    }

    static task<void>
    void_throws_exception()
    {
        throw test_exception("void task error");
        co_return;
    }

    void
    testExceptionHandler()
    {
        bool success_called = false;
        bool error_called = false;

        run_blocking(
            [&](int) { success_called = true; },
            [&](std::exception_ptr ep) {
                error_called = true;
                BOOST_TEST(ep != nullptr);
            }
        )(throws_exception());

        BOOST_TEST(!success_called);
        BOOST_TEST(error_called);
    }

    void
    testVoidExceptionHandler()
    {
        bool success_called = false;
        bool error_called = false;

        run_blocking(
            [&]() { success_called = true; },
            [&](std::exception_ptr ep) {
                error_called = true;
                BOOST_TEST(ep != nullptr);
            }
        )(void_throws_exception());

        BOOST_TEST(!success_called);
        BOOST_TEST(error_called);
    }

    void
    testExceptionRethrown()
    {
        bool caught = false;
        try
        {
            run_blocking()(throws_exception());
        }
        catch(test_exception const& e)
        {
            caught = true;
            BOOST_TEST(std::string(e.what()) == "test error");
        }
        BOOST_TEST(caught);
    }

    void
    testOverloadedHandlerException()
    {
        bool got_value = false;
        bool got_exception = false;

        struct handler_t
        {
            bool* got_value_;
            bool* got_exception_;
            void operator()(int) { *got_value_ = true; }
            void operator()(std::exception_ptr) { *got_exception_ = true; }
        };

        run_blocking(handler_t{&got_value, &got_exception})(
            throws_exception());

        BOOST_TEST(!got_value);
        BOOST_TEST(got_exception);
    }

    //----------------------------------------------------------
    // blocking_executor Tests
    //----------------------------------------------------------

    void
    testBlockingExecutorConcept()
    {
        static_assert(Executor<blocking_executor>);
        static_assert(ExecutionContext<blocking_context>);
        BOOST_TEST(true);
    }

    //----------------------------------------------------------
    // Stop Token Tests
    //----------------------------------------------------------

    static task<bool>
    check_stop_requested()
    {
        auto token = co_await this_coro::stop_token;
        co_return token.stop_requested();
    }

    void
    testStopTokenNotRequested()
    {
        bool result = true;

        std::stop_source source;
        run_blocking(source.get_token(), [&](bool v) { result = v; })(
            check_stop_requested());

        BOOST_TEST(!result);
    }

    void
    testStopTokenRequested()
    {
        bool result = false;

        std::stop_source source;
        source.request_stop();

        run_blocking(source.get_token(), [&](bool v) { result = v; })(
            check_stop_requested());

        BOOST_TEST(result);
    }

    //----------------------------------------------------------
    // Allocator Propagation
    //----------------------------------------------------------

    static task<bool>
    check_allocator_propagated()
    {
        auto* alloc = co_await this_coro::frame_allocator;
        co_return alloc != nullptr;
    }

    void
    testAllocatorPropagation()
    {
        bool result = false;
        run_blocking([&](bool v) { result = v; })(
            check_allocator_propagated());
        BOOST_TEST(result);
    }

    //----------------------------------------------------------
    // Nested Task Tests
    //----------------------------------------------------------

    static task<int>
    nested_task()
    {
        int v = co_await returns_int();
        co_return v + 1;
    }

    void
    testNestedTask()
    {
        int result = 0;
        run_blocking([&](int v) { result = v; })(nested_task());
        BOOST_TEST_EQ(result, 43);
    }

    void
    testDestroyedContextDestroysQueued()
    {
        int down = 0;
        auto probe = [](destroy_count_t) -> task<void> { co_return; };
        auto t = probe(destroy_count_t(down));
        auto h = t.handle();
        t.release();
        {
            test::blocking_context ctx;
            continuation c{h};
            ctx.get_executor().post(c);
        }
        BOOST_TEST_EQ(down, 1);
    }

    // Work posted while the services are destroyed is destroyed too.
    void
    testPostFromServiceDestructor()
    {
        int down = 0;
        auto t = plain_probe(destroy_count_t(down));
        continuation c{t.handle()};
        t.release();
        {
            blocking_context ctx;
            ctx.make_service<dtor_poster>(ctx.get_executor(), c);
        }
        BOOST_TEST_EQ(down, 1);
    }

    // A frame its services destroy at shutdown can post to the context;
    // that work is destroyed, not written into a freed queue.
    void
    testPostDuringServiceShutdown()
    {
        int down = 0;
        int posted_down = 0;
        std::optional<strand<blocking_executor>> s;
        continuation c1;
        continuation c2;
        {
            blocking_context ctx;
            auto ex = ctx.get_executor();
            s.emplace(ex);
            auto t2 = plain_probe(destroy_count_t(posted_down));
            c2.h = t2.handle();
            t2.release();
            auto t1 = posting_probe(
                destroy_count_t(down), posts_on_destroy(ex, &c2));
            c1.h = t1.handle();
            t1.release();
            s->post(c1);
        }
        s.reset();
        BOOST_TEST_EQ(down, 1);
        BOOST_TEST_EQ(posted_down, 1);
    }

    //----------------------------------------------------------

    void
    run()
    {
        // Basic Functionality
        testVoidTaskNoHandler();
        testPostDuringServiceShutdown();
        testPostFromServiceDestructor();
        testIntTaskNoHandler();
        testIntTaskWithHandler();
        testVoidTaskWithHandler();
        testStringTaskWithHandler();
        testDualHandlers();

        // Exception Handling
        testExceptionHandler();
        testVoidExceptionHandler();
        testExceptionRethrown();
        testOverloadedHandlerException();

        // blocking_executor
        testBlockingExecutorConcept();

        // Stop Token
        testStopTokenNotRequested();
        testStopTokenRequested();

        // Allocator Propagation
        testAllocatorPropagation();

        // Nested Tasks
        testNestedTask();

        // Teardown
        testDestroyedContextDestroysQueued();
    }
};

TEST_SUITE(
    run_blocking_test,
    "boost.capy.ex.run_blocking");

} // namespace test
} // namespace capy
} // namespace boost

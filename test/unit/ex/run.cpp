//
// Copyright (c) 2025 Vinnie Falco (vinnie.falco@gmail.com)
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//
// Official repository: https://github.com/cppalliance/capy
//

// Test that header file is self-contained.
#include <boost/capy/ex/run.hpp>

#include <boost/capy/concept/io_awaitable.hpp>
#include <boost/capy/ex/executor_ref.hpp>
#include <boost/capy/ex/run_async.hpp>
#include <boost/capy/ex/this_coro.hpp>
#include <boost/capy/task.hpp>

#include "test/unit/custom_task.hpp"
#include "test/unit/test_helpers.hpp"

#include <boost/capy/ex/strand.hpp>
#include <boost/capy/ex/thread_pool.hpp>
#include <boost/capy/test/run_blocking.hpp>

#if !defined(_WIN32)
#include <fcntl.h>
#include <sys/wait.h>
#include <unistd.h>
#include <csignal>
#endif

#include <latch>
#include <memory>
#include <queue>
#include <stdexcept>
#include <thread>
#include <tuple>

namespace boost {
namespace capy {

static_assert(IoAwaitable<detail::run_awaitable_ex<task<void>, executor_ref, true>>);
static_assert(IoAwaitable<detail::run_awaitable_ex<task<int>, executor_ref, true>>);
static_assert(IoAwaitable<detail::run_awaitable<task<void>, true>>);
static_assert(IoAwaitable<detail::run_awaitable<task<int>, true>>);

using test::custom_task;

// Destroys its frame even while lent, as a non-conforming IoRunnable
// may.
struct lax_task : custom_task<void>
{
    explicit lax_task(custom_task<void>&& t) noexcept
        : custom_task<void>(std::move(t))
    {
    }

    lax_task(lax_task&&) noexcept = default;

    ~lax_task()
    {
        if(auto h = handle())
        {
            release();
            h.destroy();
        }
    }
};

static_assert(IoRunnable<lax_task>);

// custom_task passes no environment to what it awaits.
struct park_plain
{
    std::coroutine_handle<>* slot;

    bool await_ready() const noexcept { return false; }
    void await_suspend(std::coroutine_handle<> h) noexcept { *slot = h; }
    void await_resume() const noexcept {}
};

// Test allocator that wraps std::allocator
template<class T>
struct test_allocator
{
    using value_type = T;

    test_allocator() = default;

    template<class U>
    test_allocator(test_allocator<U> const&) noexcept {}

    T* allocate(std::size_t n)
    {
        return std::allocator<T>{}.allocate(n);
    }

    void deallocate(T* p, std::size_t n)
    {
        std::allocator<T>{}.deallocate(p, n);
    }
};

//----------------------------------------------------------
// run Tests
//----------------------------------------------------------

struct run_test
{
    static custom_task<int>
    custom_returns_int()
    {
        co_return 456;
    }

    static custom_task<void>
    custom_returns_void()
    {
        co_return;
    }

    static task<int>
    returns_int()
    {
        co_return 123;
    }

    static task<void>
    returns_void()
    {
        co_return;
    }

    void
    testCustomTaskType()
    {
        // Proves run works with any IoRunnable, not just capy::task
        int dispatch_count = 0;
        test_executor ex(1, dispatch_count);
        int result = 0;

        auto outer = [&]() -> task<int> {
            co_return co_await capy::run(ex)(custom_returns_int());
        };

        run_async(ex, [&](int v) { result = v; })(outer());

        BOOST_TEST_EQ(result, 456);
    }

    void
    testCustomTaskTypeVoid()
    {
        // Proves run works with void custom tasks
        int dispatch_count = 0;
        test_executor ex(1, dispatch_count);
        bool called = false;

        auto outer = [&]() -> task<void> {
            co_await capy::run(ex)(custom_returns_void());
        };

        run_async(ex, [&]() { called = true; })(outer());

        BOOST_TEST(called);
    }

    void
    testExWithStopToken()
    {
        // Test run(ex, st)
        int dispatch_count = 0;
        test_executor ex(1, dispatch_count);
        std::stop_source source;
        int result = 0;

        auto outer = [&]() -> task<int> {
            co_return co_await capy::run(ex, source.get_token())(returns_int());
        };

        run_async(ex, [&](int v) { result = v; })(outer());

        BOOST_TEST_EQ(result, 123);
    }

    void
    testStopTokenOnly()
    {
        // Test run(st) - no executor, just stop_token
        int dispatch_count = 0;
        test_executor ex(1, dispatch_count);
        std::stop_source source;
        int result = 0;

        auto outer = [&]() -> task<int> {
            co_return co_await capy::run(source.get_token())(returns_int());
        };

        run_async(ex, [&](int v) { result = v; })(outer());

        BOOST_TEST_EQ(result, 123);
    }

    void
    testMemoryResourceOnly()
    {
        // Test run(mr) - no executor, just memory_resource
        int dispatch_count = 0;
        test_executor ex(1, dispatch_count);
        int result = 0;
        auto* mr = std::pmr::get_default_resource();

        auto outer = [&]() -> task<int> {
            co_return co_await capy::run(mr)(returns_int());
        };

        run_async(ex, [&](int v) { result = v; })(outer());

        BOOST_TEST_EQ(result, 123);
    }

    void
    testExWithMemoryResource()
    {
        // Test run(ex, mr)
        int dispatch_count = 0;
        test_executor ex(1, dispatch_count);
        int result = 0;
        auto* mr = std::pmr::get_default_resource();

        auto outer = [&]() -> task<int> {
            co_return co_await capy::run(ex, mr)(returns_int());
        };

        run_async(ex, [&](int v) { result = v; })(outer());

        BOOST_TEST_EQ(result, 123);
    }

    void
    testExWithStopTokenAndMemoryResource()
    {
        // Test run(ex, st, mr)
        int dispatch_count = 0;
        test_executor ex(1, dispatch_count);
        std::stop_source source;
        int result = 0;
        auto* mr = std::pmr::get_default_resource();

        auto outer = [&]() -> task<int> {
            co_return co_await capy::run(ex, source.get_token(), mr)(returns_int());
        };

        run_async(ex, [&](int v) { result = v; })(outer());

        BOOST_TEST_EQ(result, 123);
    }

    void
    testStopTokenWithMemoryResource()
    {
        // Test run(st, mr)
        int dispatch_count = 0;
        test_executor ex(1, dispatch_count);
        std::stop_source source;
        int result = 0;
        auto* mr = std::pmr::get_default_resource();

        auto outer = [&]() -> task<int> {
            co_return co_await capy::run(source.get_token(), mr)(returns_int());
        };

        run_async(ex, [&](int v) { result = v; })(outer());

        BOOST_TEST_EQ(result, 123);
    }

    void
    testExWithAllocator()
    {
        // Test run(ex, alloc) - standard allocator with executor
        int dispatch_count = 0;
        test_executor ex(1, dispatch_count);
        int result = 0;
        test_allocator<std::byte> alloc;

        auto outer = [&]() -> task<int> {
            co_return co_await capy::run(ex, alloc)(returns_int());
        };

        run_async(ex, [&](int v) { result = v; })(outer());

        BOOST_TEST_EQ(result, 123);
    }

    void
    testExWithStopTokenAndAllocator()
    {
        // Test run(ex, st, alloc) - executor + stop token + standard allocator
        int dispatch_count = 0;
        test_executor ex(1, dispatch_count);
        std::stop_source source;
        int result = 0;
        test_allocator<std::byte> alloc;

        auto outer = [&]() -> task<int> {
            co_return co_await capy::run(ex, source.get_token(), alloc)(returns_int());
        };

        run_async(ex, [&](int v) { result = v; })(outer());

        BOOST_TEST_EQ(result, 123);
    }

    void
    testAllocatorOnly()
    {
        // Test run(alloc) - standard allocator only (no executor)
        int dispatch_count = 0;
        test_executor ex(1, dispatch_count);
        int result = 0;
        test_allocator<std::byte> alloc;

        auto outer = [&]() -> task<int> {
            co_return co_await capy::run(alloc)(returns_int());
        };

        run_async(ex, [&](int v) { result = v; })(outer());

        BOOST_TEST_EQ(result, 123);
    }

    void
    testStopTokenWithAllocator()
    {
        // Test run(st, alloc) - stop token + standard allocator (no executor)
        int dispatch_count = 0;
        test_executor ex(1, dispatch_count);
        std::stop_source source;
        int result = 0;
        test_allocator<std::byte> alloc;

        auto outer = [&]() -> task<int> {
            co_return co_await capy::run(source.get_token(), alloc)(returns_int());
        };

        run_async(ex, [&](int v) { result = v; })(outer());

        BOOST_TEST_EQ(result, 123);
    }

    void
    testVoidWithStopToken()
    {
        // Test run(ex, st) with void return type
        int dispatch_count = 0;
        test_executor ex(1, dispatch_count);
        std::stop_source source;
        bool called = false;

        auto outer = [&]() -> task<void> {
            co_await capy::run(ex, source.get_token())(returns_void());
        };

        run_async(ex, [&]() { called = true; })(outer());

        BOOST_TEST(called);
    }

    void
    testVoidWithMemoryResource()
    {
        // Test run(mr) with void return type
        int dispatch_count = 0;
        test_executor ex(1, dispatch_count);
        bool called = false;
        auto* mr = std::pmr::get_default_resource();

        auto outer = [&]() -> task<void> {
            co_await capy::run(mr)(returns_void());
        };

        run_async(ex, [&]() { called = true; })(outer());

        BOOST_TEST(called);
    }

    //----------------------------------------------------------
    // Stop Token Propagation
    //----------------------------------------------------------

    static task<bool>
    check_stop_requested()
    {
        auto token = co_await this_coro::stop_token;
        co_return token.stop_requested();
    }

    void
    testStopTokenInheritance()
    {
        // Verify run(ex) inherits the caller's stop token
        int dispatch_count = 0;
        test_executor ex(1, dispatch_count);
        std::stop_source source;
        source.request_stop();
        bool result = false;

        auto outer = [&]() -> task<bool> {
            // run(ex) with no explicit stop token should inherit
            // the caller's token (which is stopped)
            co_return co_await capy::run(ex)(check_stop_requested());
        };

        run_async(ex, source.get_token(),
            [&](bool v) { result = v; })(outer());

        BOOST_TEST(result);
    }

    void
    testStopTokenOverrideInnerStopped()
    {
        // Stop the inner (override) token only.
        // Inner task should see stopped; outer should not.
        int dispatch_count = 0;
        test_executor ex(1, dispatch_count);
        std::stop_source caller_source;
        std::stop_source override_source;
        override_source.request_stop();

        bool outer_stopped = true;
        bool inner_stopped = false;

        auto outer = [&]() -> task<void> {
            auto token = co_await this_coro::stop_token;
            outer_stopped = token.stop_requested();
            inner_stopped = co_await capy::run(ex, override_source.get_token())(
                check_stop_requested());
        };

        run_async(ex, caller_source.get_token())(outer());

        BOOST_TEST(!outer_stopped);
        BOOST_TEST(inner_stopped);
    }

    void
    testStopTokenOverrideOuterStopped()
    {
        // Stop the outer (caller) token only.
        // Outer task should see stopped; inner (override) should not.
        int dispatch_count = 0;
        test_executor ex(1, dispatch_count);
        std::stop_source caller_source;
        caller_source.request_stop();
        std::stop_source override_source;

        bool outer_stopped = false;
        bool inner_stopped = true;

        auto outer = [&]() -> task<void> {
            auto token = co_await this_coro::stop_token;
            outer_stopped = token.stop_requested();
            inner_stopped = co_await capy::run(ex, override_source.get_token())(
                check_stop_requested());
        };

        run_async(ex, caller_source.get_token())(outer());

        BOOST_TEST(outer_stopped);
        BOOST_TEST(!inner_stopped);
    }

    //----------------------------------------------------------
    // Allocator Propagation
    //----------------------------------------------------------

    void
    testAllocatorPropagation()
    {
        // Verify allocator is non-null inside task launched via run_async
        int dispatch_count = 0;
        test_executor ex(1, dispatch_count);
        bool result = false;

        auto check = []() -> task<bool> {
            auto* alloc = co_await this_coro::frame_allocator;
            co_return alloc != nullptr;
        };

        run_async(ex, [&](bool v) { result = v; })(check());

        BOOST_TEST(result);
    }

    void
    testAllocatorPropagationThroughRun()
    {
        // Verify allocator propagates through run(ex) to inner task
        int dispatch_count = 0;
        test_executor ex(1, dispatch_count);
        bool result = false;

        auto inner = []() -> task<bool> {
            auto* alloc = co_await this_coro::frame_allocator;
            co_return alloc != nullptr;
        };

        auto outer = [&]() -> task<bool> {
            co_return co_await capy::run(ex)(inner());
        };

        run_async(ex, [&](bool v) { result = v; })(outer());

        BOOST_TEST(result);
    }

    void
    testRunExStrandFirstInstruction()
    {
        // Verify that the first instructions of a task passed
        // to run(strand) execute inside the strand's serialization,
        // not inline on an unprotected thread.
        thread_pool pool(2, "str-pool-");
        strand s(pool.get_executor());
        bool inside_strand = false;
        std::latch done(1);

        auto inner = [&]() -> task<void> {
            inside_strand = s.running_in_this_thread();
            co_return;
        };

        auto outer = [&]() -> task<void> {
            co_await capy::run(s)(inner());
        };

        run_async(pool.get_executor(),
            [&]() { done.count_down(); })(outer());
        done.wait();

        BOOST_TEST(inside_strand);
        pool.join();
    }

    // co_await run(compute_exec)(...) from an io loop must return
    // the caller to the io thread, not leave it on a compute worker.
    void
    testHopsBackToIoThread()
    {
        thread_pool compute_pool(2, "compute-");

        std::thread::id io_tid = std::this_thread::get_id();
        std::thread::id compute_tid{};
        std::thread::id parent_tid_after_run{};

        test::run_blocking()([&]() -> task<void> {
            auto compute_exec = compute_pool.get_executor();

            co_await capy::run(compute_exec)([&]() -> task<void> {
                compute_tid = std::this_thread::get_id();
                co_return;
            }());

            parent_tid_after_run = std::this_thread::get_id();
        }());

        BOOST_TEST(compute_tid != std::thread::id{});
        BOOST_TEST(compute_tid != io_tid);
        BOOST_TEST_EQ(parent_tid_after_run, io_tid);

        compute_pool.join();
    }

    // Executor that queues posts and counts outstanding work.
    struct counting_queue_executor
    {
        std::queue<std::coroutine_handle<>>* q_;
        int* work_;

        bool operator==(counting_queue_executor const& o) const noexcept
        {
            return q_ == o.q_;
        }
        test_io_context& context() const noexcept
        {
            return default_test_io_context();
        }
        void on_work_started() const noexcept { ++*work_; }
        void on_work_finished() const noexcept { --*work_; }
        std::coroutine_handle<> dispatch(continuation& c) const
        {
            q_->push(c.h);
            return std::noop_coroutine();
        }
        void post(continuation& c) const
        {
            q_->push(c.h);
        }
    };

    // Tearing down the inner chain posts the parent's destruction to
    // the caller's executor; running it destroys the parent there.
    void testInnerTeardownReapsParentOnCallerExecutor()
    {
        std::queue<std::coroutine_handle<>> q;
        int work = 0, child_down = 0, parent_down = 0, n = 0;
        std::coroutine_handle<> slot;
        counting_queue_executor caller{&q, &work};
        test_executor inner(n);
        auto child = [](std::coroutine_handle<>* s, destroy_count_t) -> task<void>
        {
            co_await park_awaitable{s};
        };
        auto parent = [&](destroy_count_t) -> task<void>
        {
            co_await capy::run(inner)(child(&slot, destroy_count_t(child_down)));
        };
        run_async(caller)(parent(destroy_count_t(parent_down)));
        pump(q);
        BOOST_TEST(slot);
        BOOST_TEST_EQ(work, 2); // run_async's guard and run(ex)'s hold

        slot.destroy();
        BOOST_TEST_EQ(child_down, 1);
        BOOST_TEST_EQ(parent_down, 0); // not inline: queued on caller
        BOOST_TEST_EQ(q.size(), 1u);

        pump(q);
        BOOST_TEST_EQ(parent_down, 1);
        BOOST_TEST_EQ(work, 0);
    }

    // A caller context that drains the reaper instead of running it
    // still destroys the parent.
    void testDrainedReaperDestroysParent()
    {
        std::queue<std::coroutine_handle<>> q;
        int work = 0, parent_down = 0, n = 0;
        std::coroutine_handle<> slot;
        counting_queue_executor caller{&q, &work};
        test_executor inner(n);
        auto child = [](std::coroutine_handle<>* s) -> task<void>
        {
            co_await park_awaitable{s};
        };
        auto parent = [&](destroy_count_t) -> task<void>
        {
            co_await capy::run(inner)(child(&slot));
        };
        run_async(caller)(parent(destroy_count_t(parent_down)));
        pump(q);
        slot.destroy();
        BOOST_TEST_EQ(q.size(), 1u);
        q.front().destroy();
        q.pop();
        BOOST_TEST_EQ(parent_down, 1);
    }

    // run(ex) holds work on the caller's executor until the parent is
    // resumed.
    void testRunHoldsCallerWork()
    {
        std::queue<std::coroutine_handle<>> q;
        int work = 0, n = 0;
        std::coroutine_handle<> slot;
        counting_queue_executor caller{&q, &work};
        test_executor inner(n);
        auto child = [](std::coroutine_handle<>* s) -> task<void>
        {
            co_await park_awaitable{s};
        };
        auto parent = [&]() -> task<void>
        {
            co_await capy::run(inner)(child(&slot));
        };
        run_async(caller)(parent());
        pump(q);
        BOOST_TEST_EQ(work, 2);
        slot.resume();          // child finishes, parent posted
        BOOST_TEST_EQ(work, 2); // still held while the parent is queued
        BOOST_TEST_EQ(q.size(), 1u);
        q.front().resume();     // parent resumes, run(ex) releases
        q.pop();
        BOOST_TEST_EQ(work, 0);
        pump(q);
        BOOST_TEST_EQ(work, 0);
    }

    // A caller context that drains the parent instead of resuming it
    // still releases run(ex)'s hold.
    void testDrainedParentReleasesWork()
    {
        std::queue<std::coroutine_handle<>> q;
        int work = 0, parent_down = 0, n = 0;
        std::coroutine_handle<> slot;
        counting_queue_executor caller{&q, &work};
        test_executor inner(n);
        auto child = [](std::coroutine_handle<>* s) -> task<void>
        {
            co_await park_awaitable{s};
        };
        auto parent = [&](destroy_count_t) -> task<void>
        {
            co_await capy::run(inner)(child(&slot));
        };
        run_async(caller)(parent(destroy_count_t(parent_down)));
        pump(q);
        slot.resume();
        BOOST_TEST_EQ(q.size(), 1u);
        q.front().destroy();
        q.pop();
        BOOST_TEST_EQ(parent_down, 1);
        BOOST_TEST_EQ(work, 0);
    }

    struct throwing_post_executor
    {
        bool operator==(throwing_post_executor const&) const noexcept
        {
            return true;
        }
        test_io_context& context() const noexcept
        {
            return default_test_io_context();
        }
        void on_work_started() const noexcept {}
        void on_work_finished() const noexcept {}
        std::coroutine_handle<> dispatch(continuation&) const
        {
            throw std::runtime_error("dispatch");
        }
        void post(continuation&) const
        {
            throw std::runtime_error("post");
        }
    };

    // An executor refusing the task leaves the awaiting coroutine to
    // handle the error with nothing lent and no work held on its own
    // executor.
    void testRunExPostThrows()
    {
        std::queue<std::coroutine_handle<>> q;
        int work = 0, child_down = 0;
        bool caught = false;
        counting_queue_executor caller{&q, &work};
        io_env env{executor_ref(caller), {}, nullptr};
        auto child = [](destroy_count_t) -> task<void> { co_return; };
        {
            auto aw = capy::run(throwing_post_executor{})(
                child(destroy_count_t(child_down)));
            try
            {
                std::ignore = aw.await_suspend(std::noop_coroutine(), &env);
            }
            catch(std::runtime_error const&)
            {
                caught = true;
            }
            BOOST_TEST_EQ(work, 0);
            BOOST_TEST_EQ(child_down, 0);
        }
        BOOST_TEST(caught);
        BOOST_TEST_EQ(child_down, 1);
        BOOST_TEST(q.empty());
    }

#if !defined(_WIN32)
    // Runs fn in a child process; true when the child terminated
    // abnormally (std::terminate raises SIGABRT).
    template<class F>
    static bool
    terminates(F fn)
    {
        pid_t pid = ::fork();
        if(pid == 0)
        {
            // Keep the terminate banner out of the suite's output.
            int fd = ::open("/dev/null", O_WRONLY);
            if(fd >= 0)
                ::dup2(fd, 2);
            fn();
            ::_exit(0);
        }
        int status = 0;
        ::waitpid(pid, &status, 0);
        return WIFSIGNALED(status) && WTERMSIG(status) == SIGABRT;
    }

    // Start a parent by hand without lending it, so destroying its
    // task object destroys it from above.
    template<class F>
    static void
    destroy_from_above(F parent)
    {
        int n = 0;
        test_executor caller(n);
        io_env env{executor_ref(caller), {}, nullptr};
        auto t = parent();
        t.handle().promise().set_environment(&env);
        t.handle().resume();
    }

    // Destroying a parent from above while run(ex) has its task lent
    // terminates, whatever the task type does when destroyed.
    void testDestroyParentAwaitingRunExTerminates()
    {
        BOOST_TEST(terminates([] {
            std::coroutine_handle<> slot;
            int n = 0;
            test_executor inner(n);
            auto child = [](std::coroutine_handle<>* s) -> custom_task<void>
            {
                co_await park_plain{s};
            };
            destroy_from_above([&]() -> task<void>
            {
                co_await capy::run(inner)(lax_task(child(&slot)));
            });
        }));
    }

    void testDestroyParentAwaitingRunTerminates()
    {
        BOOST_TEST(terminates([] {
            std::coroutine_handle<> slot;
            auto child = [](std::coroutine_handle<>* s) -> custom_task<void>
            {
                co_await park_plain{s};
            };
            destroy_from_above([&]() -> task<void>
            {
                co_await capy::run(std::stop_token{})(
                    lax_task(child(&slot)));
            });
        }));
    }
#endif

    void
    run()
    {
        testCustomTaskType();
        testCustomTaskTypeVoid();
        testExWithStopToken();
        testStopTokenOnly();
        testMemoryResourceOnly();
        testExWithMemoryResource();
        testExWithStopTokenAndMemoryResource();
        testStopTokenWithMemoryResource();
        testExWithAllocator();
        testExWithStopTokenAndAllocator();
        testAllocatorOnly();
        testStopTokenWithAllocator();
        testVoidWithStopToken();
        testVoidWithMemoryResource();
        testStopTokenInheritance();
        testStopTokenOverrideInnerStopped();
        testStopTokenOverrideOuterStopped();
        testAllocatorPropagation();
        testAllocatorPropagationThroughRun();
        testRunExStrandFirstInstruction();
        testHopsBackToIoThread();
        testInnerTeardownReapsParentOnCallerExecutor();
        testDrainedReaperDestroysParent();
        testRunHoldsCallerWork();
        testDrainedParentReleasesWork();
        testRunExPostThrows();
#if !defined(_WIN32)
        testDestroyParentAwaitingRunExTerminates();
        testDestroyParentAwaitingRunTerminates();
#endif
    }
};

TEST_SUITE(
    run_test,
    "boost.capy.ex.run");

} // capy
} // boost

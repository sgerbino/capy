//
// Copyright (c) 2026 Steve Gerbino
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//
// Official repository: https://github.com/cppalliance/capy
//

// Test that header file is self-contained.
#include <boost/capy/ex/coroutine_chain_holder.hpp>

#include <boost/capy/ex/any_executor.hpp>
#include <boost/capy/ex/executor_ref.hpp>
#include <boost/capy/ex/frame_allocator.hpp>
#include <boost/capy/ex/run_async.hpp>
#include <boost/capy/task.hpp>

#include <coroutine>
#include <exception>
#include <memory_resource>
#include <queue>
#include <stdexcept>
#include <type_traits>
#include <utility>

#include "test_helpers.hpp"

namespace boost {
namespace capy {

static_assert(sizeof(coroutine_chain_holder) == sizeof(void*));
static_assert(!std::is_copy_constructible_v<coroutine_chain_holder>);
static_assert(!std::is_copy_assignable_v<coroutine_chain_holder>);
static_assert(std::is_nothrow_move_constructible_v<coroutine_chain_holder>);
static_assert(std::is_nothrow_move_assignable_v<coroutine_chain_holder>);

namespace {

// A closed executor as an author would write it: it returns without
// taking the holder and does nothing else.
struct dropping_executor
{
    std::queue<continuation>* q;
    bool* closed;

    bool operator==(dropping_executor const& o) const noexcept
    {
        return q == o.q;
    }
    test_io_context& context() const noexcept
    {
        return default_test_io_context();
    }
    void on_work_started() const noexcept {}
    void on_work_finished() const noexcept {}
    std::coroutine_handle<> dispatch(coroutine_chain_holder&& h) const
    {
        post(std::move(h));
        return std::noop_coroutine();
    }
    void post(coroutine_chain_holder&& h) const
    {
        if(*closed)
            return;
        q->push(*h.release());
    }
};

static_assert(Executor<dropping_executor>);

struct throwing_executor
{
    bool operator==(throwing_executor const&) const noexcept
    {
        return true;
    }
    test_io_context& context() const noexcept
    {
        return default_test_io_context();
    }
    void on_work_started() const noexcept {}
    void on_work_finished() const noexcept {}
    std::coroutine_handle<> dispatch(coroutine_chain_holder&&) const
    {
        throw std::runtime_error("dispatch");
    }
    void post(coroutine_chain_holder&&) const
    {
        throw std::runtime_error("post");
    }
};

static_assert(Executor<throwing_executor>);

// Runs to its end on one resume and frees itself.
struct one_shot
{
    struct promise_type
    {
        one_shot get_return_object()
        {
            return {std::coroutine_handle<promise_type>::from_promise(*this)};
        }
        std::suspend_always initial_suspend() noexcept { return {}; }
        std::suspend_never final_suspend() noexcept { return {}; }
        void return_void() noexcept {}
        void unhandled_exception() { std::terminate(); }
    };

    std::coroutine_handle<promise_type> h;
};

} // namespace

struct coroutine_chain_holder_test
{
    void testEmpty()
    {
        coroutine_chain_holder h;
        BOOST_TEST(!h);
        BOOST_TEST(h.release() == nullptr);
    }

    void testDestructorAbandons()
    {
        int down = 0;
        auto probe = [](destroy_count_t) -> task<void> { co_return; };
        auto t = probe(destroy_count_t(down));
        continuation c{t.handle()};
        t.release();
        {
            coroutine_chain_holder h(c);
            BOOST_TEST(bool(h));
        }
        BOOST_TEST_EQ(down, 1);
    }

    void testReleaseLeavesTheCoroutine()
    {
        int down = 0;
        auto probe = [](destroy_count_t) -> task<void> { co_return; };
        {
            auto t = probe(destroy_count_t(down));
            continuation c{t.handle()};
            {
                coroutine_chain_holder h(c);
                BOOST_TEST(h.release() == &c);
            }
            BOOST_TEST_EQ(down, 0);
        }
        BOOST_TEST_EQ(down, 1);
    }

    void testResumeEndsOwnership()
    {
        int ran = 0;
        auto body = [](int* n) -> one_shot
        {
            ++*n;
            co_return;
        };
        continuation c{body(&ran).h};
        coroutine_chain_holder h(c);
        h.resume();
        BOOST_TEST_EQ(ran, 1);
        BOOST_TEST(!h);
    }

    void testResumeRestoresFrameAllocator()
    {
        auto body = []() -> one_shot
        {
            set_current_frame_allocator(std::pmr::null_memory_resource());
            co_return;
        };
        auto* const before = get_current_frame_allocator();
        set_current_frame_allocator(std::pmr::new_delete_resource());
        continuation c{body().h};
        coroutine_chain_holder(c).resume();
        BOOST_TEST(
            get_current_frame_allocator() == std::pmr::new_delete_resource());
        set_current_frame_allocator(before);
    }

    void testTakeHandleEndsOwnership()
    {
        int down = 0;
        auto probe = [](destroy_count_t) -> task<void> { co_return; };
        {
            auto t = probe(destroy_count_t(down));
            continuation c{t.handle()};
            {
                coroutine_chain_holder h(c);
                BOOST_TEST(h.take_handle() == t.handle());
                BOOST_TEST(!h);
            }
            BOOST_TEST_EQ(down, 0);
        }
        BOOST_TEST_EQ(down, 1);
    }

    // The second claim on a field is the double abandon this prevents.
    void testTakeClearsTheField()
    {
        continuation c{std::noop_coroutine()};
        continuation* field = &c;
        auto first = coroutine_chain_holder::take(field);
        auto second = coroutine_chain_holder::take(field);
        BOOST_TEST(field == nullptr);
        BOOST_TEST(!second);
        BOOST_TEST(first.release() == &c);
    }

    void testResumeViaPosts()
    {
        int down = 0;
        auto probe = [](destroy_count_t) -> task<void> { co_return; };
        {
            auto t = probe(destroy_count_t(down));
            continuation c{t.handle()};
            bool closed = false;
            std::queue<continuation> q;
            resume_via(dropping_executor{&q, &closed}, c);
            BOOST_TEST_EQ(q.size(), 1u);
            BOOST_TEST_EQ(down, 0);
        }
        BOOST_TEST_EQ(down, 1);
    }

    void testResumeViaGivesUpOnThrow()
    {
        int down = 0;
        auto probe = [](destroy_count_t) -> task<void> { co_return; };
        auto t = probe(destroy_count_t(down));
        continuation c{t.handle()};
        t.release();
        BOOST_TEST_THROWS(
            resume_via(throwing_executor{}, c), std::runtime_error);
        BOOST_TEST_EQ(down, 1);
    }

    void testMoveTransfersOwnership()
    {
        int down = 0;
        auto probe = [](destroy_count_t) -> task<void> { co_return; };
        auto t = probe(destroy_count_t(down));
        continuation c{t.handle()};
        t.release();
        coroutine_chain_holder a(c);
        {
            coroutine_chain_holder b(std::move(a));
            BOOST_TEST(!a);
            BOOST_TEST(bool(b));
            BOOST_TEST_EQ(down, 0);
        }
        BOOST_TEST_EQ(down, 1);
    }

    void testMoveAssignAbandonsTheOld()
    {
        int a_down = 0, b_down = 0;
        auto probe = [](destroy_count_t) -> task<void> { co_return; };
        auto ta = probe(destroy_count_t(a_down));
        auto tb = probe(destroy_count_t(b_down));
        continuation ca{ta.handle()};
        continuation cb{tb.handle()};
        ta.release();
        tb.release();
        {
            coroutine_chain_holder a(ca);
            coroutine_chain_holder b(cb);
            a = std::move(b);
            BOOST_TEST_EQ(a_down, 1);
            BOOST_TEST_EQ(b_down, 0);
        }
        BOOST_TEST_EQ(b_down, 1);
    }

    void testNoopCoroutineIsSafeToDrop()
    {
        continuation c{std::noop_coroutine()};
        coroutine_chain_holder h(c);
    }

    void testFailedPostLeavesTheCaller()
    {
        int down = 0;
        auto probe = [](destroy_count_t) -> task<void> { co_return; };
        {
            auto t = probe(destroy_count_t(down));
            continuation c{t.handle()};
            throwing_executor raw;
            executor_ref ref(raw);
            any_executor any(raw);

            coroutine_chain_holder h(c);
            BOOST_TEST_THROWS(raw.post(std::move(h)), std::runtime_error);
            BOOST_TEST(bool(h));
            BOOST_TEST_THROWS(ref.post(std::move(h)), std::runtime_error);
            BOOST_TEST(bool(h));
            BOOST_TEST_THROWS(ref.dispatch(std::move(h)), std::runtime_error);
            BOOST_TEST(bool(h));
            BOOST_TEST_THROWS(any.post(std::move(h)), std::runtime_error);
            BOOST_TEST(bool(h));
            BOOST_TEST_THROWS(any.dispatch(std::move(h)), std::runtime_error);
            BOOST_TEST(bool(h));
            h.release();
            BOOST_TEST_EQ(down, 0);
        }
        BOOST_TEST_EQ(down, 1);
    }

    // The bug the type exists to prevent: an author's closed executor
    // does nothing with what it is given, and the chain still unwinds.
    void testDroppedPostUnwindsTheChain()
    {
        int child_down = 0, parent_down = 0;
        bool closed = false;
        parked slot;
        std::queue<continuation> q;
        dropping_executor ex{&q, &closed};
        auto child = [](parked* s, destroy_count_t) -> task<void>
        {
            co_await park_awaitable{s};
        };
        auto parent = [&](parked* s, destroy_count_t) -> task<void>
        {
            co_await child(s, destroy_count_t(child_down));
        };
        run_async(ex)(parent(&slot, destroy_count_t(parent_down)));
        pump(q);
        BOOST_TEST(bool(slot));

        closed = true;
        executor_ref(ex).post(coroutine_chain_holder(slot));
        BOOST_TEST_EQ(child_down, 1);
        BOOST_TEST_EQ(parent_down, 1);
    }

    void run()
    {
        testEmpty();
        testDestructorAbandons();
        testReleaseLeavesTheCoroutine();
        testResumeEndsOwnership();
        testResumeRestoresFrameAllocator();
        testTakeHandleEndsOwnership();
        testTakeClearsTheField();
        testResumeViaPosts();
        testResumeViaGivesUpOnThrow();
        testMoveTransfersOwnership();
        testMoveAssignAbandonsTheOld();
        testNoopCoroutineIsSafeToDrop();
        testFailedPostLeavesTheCaller();
        testDroppedPostUnwindsTheChain();
    }
};

TEST_SUITE(
    coroutine_chain_holder_test,
    "boost.capy.coroutine_chain_holder");

} // namespace capy
} // namespace boost

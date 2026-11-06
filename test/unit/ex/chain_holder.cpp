//
// Copyright (c) 2026 Steve Gerbino
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//
// Official repository: https://github.com/cppalliance/capy
//

// Test that header file is self-contained.
#include <boost/capy/ex/chain_holder.hpp>

#include <boost/capy/ex/any_executor.hpp>
#include <boost/capy/ex/executor_ref.hpp>
#include <boost/capy/ex/run_async.hpp>
#include <boost/capy/task.hpp>

#include <coroutine>
#include <queue>
#include <stdexcept>
#include <type_traits>
#include <utility>

#include "test_helpers.hpp"

namespace boost {
namespace capy {

static_assert(sizeof(chain_holder) == sizeof(void*));
static_assert(!std::is_copy_constructible_v<chain_holder>);
static_assert(!std::is_copy_assignable_v<chain_holder>);
static_assert(std::is_nothrow_move_constructible_v<chain_holder>);
static_assert(std::is_nothrow_move_assignable_v<chain_holder>);

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
    std::coroutine_handle<> dispatch(chain_holder&& h) const
    {
        post(std::move(h));
        return std::noop_coroutine();
    }
    void post(chain_holder&& h) const
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
    std::coroutine_handle<> dispatch(chain_holder&&) const
    {
        throw std::runtime_error("dispatch");
    }
    void post(chain_holder&&) const
    {
        throw std::runtime_error("post");
    }
};

static_assert(Executor<throwing_executor>);

} // namespace

struct chain_holder_test
{
    void testEmpty()
    {
        chain_holder h;
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
            chain_holder h(c);
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
                chain_holder h(c);
                BOOST_TEST(h.release() == &c);
            }
            BOOST_TEST_EQ(down, 0);
        }
        BOOST_TEST_EQ(down, 1);
    }

    void testMoveTransfersOwnership()
    {
        int down = 0;
        auto probe = [](destroy_count_t) -> task<void> { co_return; };
        auto t = probe(destroy_count_t(down));
        continuation c{t.handle()};
        t.release();
        chain_holder a(c);
        {
            chain_holder b(std::move(a));
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
            chain_holder a(ca);
            chain_holder b(cb);
            a = std::move(b);
            BOOST_TEST_EQ(a_down, 1);
            BOOST_TEST_EQ(b_down, 0);
        }
        BOOST_TEST_EQ(b_down, 1);
    }

    void testNoopCoroutineIsSafeToDrop()
    {
        continuation c{std::noop_coroutine()};
        chain_holder h(c);
    }

    void testTransferConsumesOnSuccess()
    {
        continuation c{std::noop_coroutine()};
        chain_holder h(c);
        continuation* seen = nullptr;
        int const r = h.transfer([&](continuation& x)
        {
            seen = &x;
            return 7;
        });
        BOOST_TEST_EQ(r, 7);
        BOOST_TEST(seen == &c);
        BOOST_TEST(!h);
    }

    void testTransferRestoresOnThrow()
    {
        continuation c{std::noop_coroutine()};
        chain_holder h(c);
        BOOST_TEST_THROWS(
            h.transfer([](continuation&) { throw std::runtime_error("x"); }),
            std::runtime_error);
        BOOST_TEST(bool(h));
        BOOST_TEST(h.release() == &c);
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

            chain_holder h(c);
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
        executor_ref(ex).post(chain_holder(slot));
        BOOST_TEST_EQ(child_down, 1);
        BOOST_TEST_EQ(parent_down, 1);
    }

    void run()
    {
        testEmpty();
        testDestructorAbandons();
        testReleaseLeavesTheCoroutine();
        testMoveTransfersOwnership();
        testMoveAssignAbandonsTheOld();
        testNoopCoroutineIsSafeToDrop();
        testTransferConsumesOnSuccess();
        testTransferRestoresOnThrow();
        testFailedPostLeavesTheCaller();
        testDroppedPostUnwindsTheChain();
    }
};

TEST_SUITE(
    chain_holder_test,
    "boost.capy.chain_holder");

} // namespace capy
} // namespace boost

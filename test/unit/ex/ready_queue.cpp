//
// Copyright (c) 2026 Steve Gerbino
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//
// Official repository: https://github.com/cppalliance/capy
//

// Test that header file is self-contained.
#include <boost/capy/ex/ready_queue.hpp>

#include <boost/capy/task.hpp>

#include <coroutine>
#include <type_traits>
#include <utility>

#include "test_helpers.hpp"

namespace boost {
namespace capy {

namespace {

struct op : ready_node
{
    int id = 0;
    explicit op(int i) : id(i) {}
};

using mixed_queue = ready_queue<op>;

// The hot queue must stay two pointers with no tag handling.
static_assert(sizeof(ready_queue<>) == 2 * sizeof(void*));
static_assert(!std::is_copy_constructible_v<ready_queue<>>);
static_assert(std::is_nothrow_move_constructible_v<ready_queue<>>);
static_assert(sizeof(mixed_queue) == 2 * sizeof(void*));

} // namespace

struct ready_queue_test
{
    void testFifo()
    {
        continuation a{std::noop_coroutine()};
        continuation b{std::noop_coroutine()};
        continuation c{std::noop_coroutine()};
        ready_queue<> q;
        BOOST_TEST(q.empty());
        q.push(chain_holder(a));
        q.push(chain_holder(b));
        q.push(chain_holder(c));
        BOOST_TEST(!q.empty());
        BOOST_TEST(q.pop().release() == &a);
        BOOST_TEST(q.pop().release() == &b);
        BOOST_TEST(q.pop().release() == &c);
        BOOST_TEST(q.empty());
        BOOST_TEST(!q.pop());
    }

    void testPushAfterEmptied()
    {
        continuation a{std::noop_coroutine()};
        ready_queue<> q;
        q.push(chain_holder(a));
        q.pop().release();
        q.push(chain_holder(a));
        BOOST_TEST(q.pop().release() == &a);
        BOOST_TEST(q.empty());
    }

    void testPushTakesTheHolder()
    {
        continuation a{std::noop_coroutine()};
        ready_queue<> q;
        chain_holder h(a);
        q.push(std::move(h));
        BOOST_TEST(!h);
        q.pop().release();
    }

    void testDestructorAbandonsEach()
    {
        int down = 0;
        auto probe = [](destroy_count_t) -> task<void> { co_return; };
        auto t1 = probe(destroy_count_t(down));
        auto t2 = probe(destroy_count_t(down));
        continuation c1{t1.handle()};
        continuation c2{t2.handle()};
        t1.release();
        t2.release();
        {
            ready_queue<> q;
            q.push(chain_holder(c1));
            q.push(chain_holder(c2));
        }
        BOOST_TEST_EQ(down, 2);
    }

    void testSplice()
    {
        continuation a{std::noop_coroutine()};
        continuation b{std::noop_coroutine()};
        continuation c{std::noop_coroutine()};
        ready_queue<> q1, q2, empty;
        q1.push(chain_holder(a));
        q2.push(chain_holder(b));
        q2.push(chain_holder(c));
        q1.splice(empty);
        q1.splice(q2);
        BOOST_TEST(q2.empty());
        q2.splice(q1);
        BOOST_TEST(q1.empty());
        BOOST_TEST(q2.pop().release() == &a);
        BOOST_TEST(q2.pop().release() == &b);
        BOOST_TEST(q2.pop().release() == &c);
    }

    void testMove()
    {
        continuation a{std::noop_coroutine()};
        ready_queue<> q1;
        q1.push(chain_holder(a));
        ready_queue<> q2(std::move(q1));
        BOOST_TEST(q1.empty());
        BOOST_TEST(q2.pop().release() == &a);
    }

    void testMixedKeepsOneOrder()
    {
        op a(1), b(2);
        continuation c{std::noop_coroutine()};
        mixed_queue q;
        q.push(&a);
        q.push(chain_holder(c));
        q.push(&b);

        auto e1 = q.pop();
        BOOST_TEST(!e1.is_continuation());
        BOOST_TEST(e1.op() == &a);

        auto e2 = q.pop();
        BOOST_TEST(e2.is_continuation());
        BOOST_TEST(e2.op() == nullptr);
        BOOST_TEST(e2.take().release() == &c);
        BOOST_TEST(!e2);

        auto e3 = q.pop();
        BOOST_TEST(e3.op() == &b);
        BOOST_TEST(q.empty());
        BOOST_TEST(!q.pop());
    }

    void testMixedSplice()
    {
        op a(1);
        continuation c{std::noop_coroutine()};
        mixed_queue q1, q2;
        q1.push(chain_holder(c));
        q2.push(&a);
        q1.splice(q2);
        BOOST_TEST(q2.empty());
        BOOST_TEST(q1.pop().take().release() == &c);
        BOOST_TEST(q1.pop().op() == &a);
    }

    void testMixedDestructorAbandonsOnlyCoroutines()
    {
        int down = 0;
        auto probe = [](destroy_count_t) -> task<void> { co_return; };
        auto t = probe(destroy_count_t(down));
        continuation c{t.handle()};
        t.release();
        op a(1);
        {
            mixed_queue q;
            q.push(&a);
            q.push(chain_holder(c));
        }
        BOOST_TEST_EQ(down, 1);
        BOOST_TEST_EQ(a.id, 1);
    }

    void testEntryAbandonsWhenDropped()
    {
        int down = 0;
        auto probe = [](destroy_count_t) -> task<void> { co_return; };
        auto t = probe(destroy_count_t(down));
        continuation c{t.handle()};
        t.release();
        mixed_queue q;
        q.push(chain_holder(c));
        {
            auto e = q.pop();
            BOOST_TEST(bool(e));
        }
        BOOST_TEST_EQ(down, 1);
    }

    void run()
    {
        testFifo();
        testPushAfterEmptied();
        testPushTakesTheHolder();
        testDestructorAbandonsEach();
        testSplice();
        testMove();
        testMixedKeepsOneOrder();
        testMixedSplice();
        testMixedDestructorAbandonsOnlyCoroutines();
        testEntryAbandonsWhenDropped();
    }
};

TEST_SUITE(
    ready_queue_test,
    "boost.capy.ready_queue");

} // namespace capy
} // namespace boost

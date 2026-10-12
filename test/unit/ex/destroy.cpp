//
// Copyright (c) 2026 Steve Gerbino
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//
// Official repository: https://github.com/cppalliance/capy
//

// Test that header file is self-contained.
#include <boost/capy/ex/destroy.hpp>

#include <boost/capy/ex/frame_allocator.hpp>
#include <boost/capy/ex/io_awaitable_promise_base.hpp>
#include <boost/capy/ex/io_env.hpp>
#include <boost/capy/ex/run_async.hpp>
#include <boost/capy/task.hpp>

#include <coroutine>
#include <cstddef>
#include <cstring>
#include <memory>
#include <memory_resource>
#include <new>
#include <queue>
#include <tuple>
#include <utility>
#include <vector>

#include "test_helpers.hpp"

namespace boost {
namespace capy {

namespace {

// Parks the awaiting coroutine without the environment protocol.
struct plain_park
{
    std::coroutine_handle<>* slot;

    bool await_ready() const noexcept { return false; }
    void await_suspend(std::coroutine_handle<> h) noexcept { *slot = h; }
    void await_resume() const noexcept {}
};

/** A coroutine type whose promise deallocates its frame itself.

    Its allocation goes through the frame allocator in scope, like
    `frame_alloc_mixin`, but its `operator delete` calls nothing else.
*/
struct own_delete_task
{
    struct promise_type
        : io_awaitable_promise_base<promise_type>
    {
        static void* operator new(std::size_t size)
        {
            auto* mr = get_current_frame_allocator();
            if(!mr)
                mr = std::pmr::get_default_resource();
            auto total = size + sizeof(std::pmr::memory_resource*);
            void* raw = mr->allocate(total, alignof(std::max_align_t));
            std::memcpy(
                static_cast<char*>(raw) + size, &mr,
                sizeof(std::pmr::memory_resource*));
            return raw;
        }

        static void operator delete(void* p, std::size_t size) noexcept
        {
            std::pmr::memory_resource* mr;
            std::memcpy(
                &mr, static_cast<char*>(p) + size,
                sizeof(std::pmr::memory_resource*));
            mr->deallocate(
                p, size + sizeof(std::pmr::memory_resource*),
                alignof(std::max_align_t));
        }

        own_delete_task get_return_object()
        {
            return own_delete_task{
                std::coroutine_handle<promise_type>::from_promise(*this)};
        }

        std::suspend_always initial_suspend() noexcept { return {}; }

        auto final_suspend() noexcept
        {
            struct awaiter
            {
                promise_type* p_;
                bool await_ready() const noexcept { return false; }
                std::coroutine_handle<> await_suspend(
                    std::coroutine_handle<>) const noexcept
                {
                    return p_->continuation();
                }
                void await_resume() const noexcept {}
            };
            return awaiter{this};
        }

        void return_void() noexcept {}
        void unhandled_exception() noexcept { std::terminate(); }
    };

    std::coroutine_handle<promise_type> h_;

    explicit own_delete_task(std::coroutine_handle<promise_type> h)
        : h_(h)
    {
    }

    own_delete_task(own_delete_task&& o) noexcept
        : h_(std::exchange(o.h_, nullptr))
    {
    }

    ~own_delete_task()
    {
        if(h_)
        {
            if(!h_.done() && h_.promise().has_owner())
                std::terminate();
            h_.destroy();
        }
    }

    void release() noexcept { h_ = nullptr; }

    bool await_ready() const noexcept { return false; }

    std::coroutine_handle<> await_suspend(
        std::coroutine_handle<> cont, io_env const* env)
    {
        h_.promise().set_continuation(cont);
        h_.promise().set_environment(env);
        h_.promise().set_owner(*this);
        return h_;
    }

    void await_resume() noexcept
    {
        h_.promise().clear_owner();
    }
};

// Destroys a parked coroutine of another chain when the frame that
// owns it as a by-value parameter is destroyed.
struct holder_param
{
    std::coroutine_handle<>* h;

    explicit holder_param(std::coroutine_handle<>& slot) noexcept
        : h(&slot)
    {
    }

    holder_param(holder_param&& o) noexcept
        : h(std::exchange(o.h, nullptr))
    {
    }

    ~holder_param()
    {
        if(h && *h)
            capy::destroy(std::exchange(*h, nullptr));
    }
};

// Reads through a pointer when its frame is destroyed.
/** Parks any number of coroutines; destroying it destroys them all.

    Plays a holder whose teardown runs inside an outer one.
*/
struct parking_lot
{
    std::vector<std::coroutine_handle<>> hs;

    ~parking_lot()
    {
        for(auto h : hs)
            capy::destroy(h);
    }

    struct awaiter
    {
        parking_lot* lot;

        bool await_ready() const noexcept { return false; }

        std::coroutine_handle<>
        await_suspend(std::coroutine_handle<> h, io_env const*)
        {
            lot->hs.push_back(h);
            return std::noop_coroutine();
        }

        void await_resume() const noexcept {}
    };

    awaiter park() noexcept { return awaiter{this}; }
};

struct reader
{
    int const* p;
    int* seen;

    ~reader() { *seen = *p; }
};

// Destroys parked coroutines of other chains with a raw destroy().
struct raw_holder
{
    std::vector<std::coroutine_handle<>>* hs;

    ~raw_holder()
    {
        for(auto h : *hs)
            h.destroy();
    }
};

} // namespace

struct destroy_test
{
    // A holder owned by a frame that the teardown destroys: every
    // parked coroutine's parent is collected, with no limit on how many.
    void testCascadeDestroysThousandWaiters()
    {
        constexpr int count = 1000;
        int inner_down = 0, outer_down = 0, server_down = 0;
        std::coroutine_handle<> slot;
        std::queue<std::coroutine_handle<>> q;
        queuing_executor ex(q);
        auto inner = [](parking_lot& l, destroy_count_t) -> task<void>
        {
            co_await l.park();
        };
        auto outer = [&](parking_lot& l, destroy_count_t) -> task<void>
        {
            co_await inner(l, destroy_count_t(inner_down));
        };
        auto leaf = [](std::coroutine_handle<>* s) -> task<void>
        {
            co_await park_awaitable{s};
        };
        auto server = [&](destroy_count_t) -> task<void>
        {
            parking_lot lot;
            for(int i = 0; i < count; ++i)
                run_async(ex)(outer(lot, destroy_count_t(outer_down)));
            co_await leaf(&slot);
        };
        run_async(ex)(server(destroy_count_t(server_down)));
        pump(q);
        BOOST_TEST(slot);
        BOOST_TEST_EQ(inner_down, 0);

        capy::destroy(slot);
        BOOST_TEST_EQ(inner_down, count);
        BOOST_TEST_EQ(outer_down, count);
        BOOST_TEST_EQ(server_down, 1);
    }

    // A holder calling h.destroy() directly still destroys the chain
    // once when the frames use the default allocator.
    void testRawDestroyFallsBackInline()
    {
        int child_down = 0, parent_down = 0, handler_down = 0;
        std::coroutine_handle<> slot;
        auto child = [](std::coroutine_handle<>* s, destroy_count_t)
            -> task<void>
        {
            co_await park_awaitable{s};
        };
        auto parent = [&](destroy_count_t) -> task<void>
        {
            co_await child(&slot, destroy_count_t(child_down));
        };
        int n = 0;
        test_executor ex(n);
        run_async(ex,
            [t = destroy_count_t(handler_down)] { BOOST_TEST(false); },
            [](std::exception_ptr) { BOOST_TEST(false); })(
            parent(destroy_count_t(parent_down)));
        BOOST_TEST(slot);

        slot.destroy();
        BOOST_TEST_EQ(child_down, 1);
        BOOST_TEST_EQ(parent_down, 1);
        BOOST_TEST_EQ(handler_down, 1);
    }

    // A promise that frees its own frame through the allocator owned by
    // the run_async trampoline calls no hook; the trampoline is still
    // destroyed only after that frame is freed.
    void testOwnOperatorDeleteNeedsNoHook()
    {
        int parent_down = 0;
        std::coroutine_handle<> slot;
        auto child = [](std::coroutine_handle<>* s) -> own_delete_task
        {
            co_await plain_park{s};
        };
        auto parent = [child](std::coroutine_handle<>* s, destroy_count_t)
            -> task<void>
        {
            co_await child(s);
        };
        int n = 0;
        test_executor ex(n);
        run_async(ex, std::allocator<std::byte>{})(
            parent(&slot, destroy_count_t(parent_down)));
        BOOST_TEST(slot);

        capy::destroy(slot);
        BOOST_TEST_EQ(parent_down, 1);
    }

    // A teardown started while a frame is between its promise and its
    // deallocation leaves that frame's parent alone: the parent's chain
    // owns the allocator the frame is about to be freed through.
    void testNestedDestroyKeepsOuterParent()
    {
        int parent_down = 0, other_down = 0, other_parent_down = 0;
        std::coroutine_handle<> slot, other;
        auto other_leaf = [](std::coroutine_handle<>* s, destroy_count_t)
            -> task<void>
        {
            co_await park_awaitable{s};
        };
        auto other_parent = [&](destroy_count_t) -> task<void>
        {
            co_await other_leaf(&other, destroy_count_t(other_down));
        };
        auto leaf = [](holder_param, std::coroutine_handle<>* s)
            -> task<void>
        {
            co_await park_awaitable{s};
        };
        auto parent = [&](destroy_count_t) -> task<void>
        {
            co_await leaf(holder_param(other), &slot);
        };
        int n = 0;
        test_executor ex(n);
        run_async(ex)(other_parent(destroy_count_t(other_parent_down)));
        run_async(ex, std::allocator<std::byte>{})(
            parent(destroy_count_t(parent_down)));
        BOOST_TEST(slot);
        BOOST_TEST(other);

        capy::destroy(slot);
        BOOST_TEST_EQ(other_down, 1);
        BOOST_TEST_EQ(other_parent_down, 1);
        BOOST_TEST_EQ(parent_down, 1);
    }

    // A teardown started inside an outer teardown destroys the parents
    // it releases before returning, while the frame that started it
    // still holds what those parents use.
    void testNestedDestroyDrainsItsOwnParents()
    {
        int seen = 0;
        std::coroutine_handle<> slot;
        std::queue<std::coroutine_handle<>> q;
        queuing_executor ex(q);
        auto inner = [](parking_lot& l) -> task<void>
        {
            co_await l.park();
        };
        auto outer = [&](parking_lot& l, int const& v) -> task<void>
        {
            reader r{&v, &seen};
            co_await inner(l);
        };
        auto leaf = [](std::coroutine_handle<>* s) -> task<void>
        {
            co_await park_awaitable{s};
        };
        auto server = [&]() -> task<void>
        {
            auto value = std::make_unique<int>(42);
            parking_lot lot;
            run_async(ex)(outer(lot, *value));
            co_await leaf(&slot);
        };
        run_async(ex)(server());
        pump(q);
        BOOST_TEST(slot);

        capy::destroy(slot);
        BOOST_TEST_EQ(seen, 42);
    }

    // Raw destroys made inside a teardown leave their parents to it,
    // however many there are.
    void testRawDestroysInsideTeardownCollectParents()
    {
        constexpr int count = 100;
        int leaf_down = 0, parent_down = 0;
        std::coroutine_handle<> slot;
        std::vector<std::coroutine_handle<>> parked(count);
        auto leaf = [](std::coroutine_handle<>* s, destroy_count_t)
            -> task<void>
        {
            co_await park_awaitable{s};
        };
        auto parent = [&](std::coroutine_handle<>* s, destroy_count_t)
            -> task<void>
        {
            co_await leaf(s, destroy_count_t(leaf_down));
        };
        auto server_leaf = [](std::coroutine_handle<>* s) -> task<void>
        {
            co_await park_awaitable{s};
        };
        auto server = [&]() -> task<void>
        {
            raw_holder holder{&parked};
            co_await server_leaf(&slot);
        };
        int n = 0;
        test_executor ex(n);
        for(auto& h : parked)
            run_async(ex)(parent(&h, destroy_count_t(parent_down)));
        run_async(ex)(server());
        BOOST_TEST(slot);

        capy::destroy(slot);
        BOOST_TEST_EQ(leaf_down, count);
        BOOST_TEST_EQ(parent_down, count);
    }

    void
    run()
    {
        testCascadeDestroysThousandWaiters();
        testRawDestroyFallsBackInline();
        testOwnOperatorDeleteNeedsNoHook();
        testNestedDestroyKeepsOuterParent();
        testNestedDestroyDrainsItsOwnParents();
        testRawDestroysInsideTeardownCollectParents();
    }
};

TEST_SUITE(
    destroy_test,
    "boost.capy.ex.destroy");

} // capy
} // boost

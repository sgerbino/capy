//
// Copyright (c) 2026 Steve Gerbino
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//
// Official repository: https://github.com/cppalliance/capy
//

#ifndef BOOST_CAPY_TEST_UNIT_EX_PRIORITY_EXECUTOR_HPP
#define BOOST_CAPY_TEST_UNIT_EX_PRIORITY_EXECUTOR_HPP

#include <boost/capy/concept/executor.hpp>
#include <boost/capy/continuation.hpp>
#include <boost/capy/ex/ready_queue.hpp>
#include <boost/capy/ex/execution_context.hpp>
#include <boost/capy/ex/executor_ref.hpp>
#include <boost/capy/ex/frame_allocator.hpp>

#include <atomic>
#include <coroutine>
#include <exception>
#include <mutex>
#include <thread>
#include <type_traits>
#include <utility>

namespace boost {
namespace capy {
namespace test {

/** Test-only strand-shaped executor that drains high before low.
*/
struct priority_executor_state
{
    std::mutex mutex;
    ready_queue<> high;
    ready_queue<> low;
    bool locked = false;
    std::atomic<std::thread::id> dispatch_thread{};
};

namespace detail {

struct priority_invoker
{
    struct promise_type
    {
        continuation self;

        priority_invoker get_return_object() noexcept
        {
            return {std::coroutine_handle<promise_type>::from_promise(*this)};
        }

        std::suspend_always initial_suspend() noexcept { return {}; }
        std::suspend_never final_suspend() noexcept { return {}; }
        void return_void() noexcept {}
        void unhandled_exception() { std::terminate(); }
    };

    std::coroutine_handle<promise_type> h_;
};

inline void
drain_list(ready_queue<>& q) noexcept
{
    while(auto h = q.pop())
        h.resume();
}

inline priority_invoker
make_priority_invoker(priority_executor_state* s)
{
    for(;;)
    {
        s->dispatch_thread.store(
            std::this_thread::get_id(),
            std::memory_order_release);

        ready_queue<> high;
        ready_queue<> low;
        {
            std::lock_guard<std::mutex> lk(s->mutex);
            high.splice(s->high);
            low.splice(s->low);
        }

        drain_list(high);
        drain_list(low);

        {
            std::lock_guard<std::mutex> lk(s->mutex);
            if(s->high.empty() && s->low.empty())
            {
                s->locked = false;
                s->dispatch_thread.store(
                    std::thread::id{},
                    std::memory_order_release);
                co_return;
            }
        }
    }
}

} // namespace detail

/** Executor view over priority_executor_state. Dispatch has the same
    thread-check fast path as strand; post defaults to the low queue.
*/
template<class Ex>
class priority_executor
{
    priority_executor_state* state_;
    Ex inner_ex_;

    enum class priority { high, low };

    void
    enqueue_under_lock(coroutine_chain_holder&& h, priority p) const noexcept
    {
        (p == priority::high ? state_->high : state_->low).push(std::move(h));
    }

    void
    post_with_priority(coroutine_chain_holder&& h, priority p) const
    {
        bool first;
        {
            std::lock_guard<std::mutex> lk(state_->mutex);
            enqueue_under_lock(std::move(h), p);
            first = !state_->locked;
            if(first) state_->locked = true;
        }
        if(first)
            post_invoker();
    }

    void
    post_invoker() const
    {
        auto inv = detail::make_priority_invoker(state_);
        auto& self = inv.h_.promise().self;
        self.h = inv.h_;
        inner_ex_.post(coroutine_chain_holder(self));
    }

public:
    priority_executor(priority_executor_state& state, Ex inner) noexcept(
        std::is_nothrow_move_constructible_v<Ex>)
        : state_(&state)
        , inner_ex_(std::move(inner))
    {
    }

    priority_executor(priority_executor const&) noexcept(
        std::is_nothrow_copy_constructible_v<Ex>) = default;
    priority_executor(priority_executor&&) noexcept(
        std::is_nothrow_move_constructible_v<Ex>) = default;
    priority_executor& operator=(priority_executor const&) = default;
    priority_executor& operator=(priority_executor&&) noexcept(
        std::is_nothrow_move_assignable_v<Ex>) = default;

    bool
    operator==(priority_executor const& other) const noexcept
    {
        return state_ == other.state_;
    }

    auto&
    context() const noexcept
    {
        return inner_ex_.context();
    }

    void on_work_started() const noexcept { inner_ex_.on_work_started(); }
    void on_work_finished() const noexcept { inner_ex_.on_work_finished(); }

    bool
    running_in_this_thread() const noexcept
    {
        return state_->dispatch_thread.load(std::memory_order_acquire)
            == std::this_thread::get_id();
    }

    std::coroutine_handle<>
    dispatch(coroutine_chain_holder&& h) const
    {
        if(running_in_this_thread())
            return h.take_handle();
        post_with_priority(std::move(h), priority::low);
        return std::noop_coroutine();
    }

    void
    post(coroutine_chain_holder&& h) const
    {
        post_with_priority(std::move(h), priority::low);
    }

    void
    post_high(coroutine_chain_holder&& h) const
    {
        post_with_priority(std::move(h), priority::high);
    }

    void
    post_low(coroutine_chain_holder&& h) const
    {
        post_with_priority(std::move(h), priority::low);
    }
};

} // namespace test
} // namespace capy
} // namespace boost

#endif

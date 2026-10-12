//
// Copyright (c) 2025 Vinnie Falco (vinnie.falco@gmail.com)
// Copyright (c) 2026 Michael Vandeberg
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//
// Official repository: https://github.com/cppalliance/capy
//

#ifndef BOOST_CAPY_EX_IO_AWAITABLE_PROMISE_BASE_HPP
#define BOOST_CAPY_EX_IO_AWAITABLE_PROMISE_BASE_HPP

#include <boost/capy/detail/config.hpp>
#include <boost/capy/ex/frame_alloc_mixin.hpp>
#include <boost/capy/ex/frame_allocator.hpp>
#include <boost/capy/ex/io_env.hpp>
#include <boost/capy/ex/this_coro.hpp>

#include <coroutine>
#include <memory_resource>
#include <stop_token>
#include <type_traits>

namespace boost {
namespace capy {

/** CRTP mixin that adds I/O awaitable support to a promise type.

    Inherit from this class to enable these capabilities in your coroutine:

    1. **Frame allocation** — The mixin provides `operator new/delete` that
       use the thread-local frame allocator set by `run_async`.

    2. **Environment storage** — The mixin stores a pointer to the `io_env`
       containing the executor, stop token, and allocator for this coroutine.

    3. **Environment access** — Coroutine code can retrieve the environment
       via `co_await this_coro::environment`, or individual fields via
       `co_await this_coro::executor`, `co_await this_coro::stop_token`,
       and `co_await this_coro::frame_allocator`.

    @tparam Derived The derived promise type (CRTP pattern).

    @par Basic Usage

    For coroutines that need to access their execution environment:

    @par !example example_1


    @par Custom Awaitable Transformation

    If your promise needs to transform awaitables (e.g., for affinity or
    logging), override `transform_awaitable` instead of `await_transform`:

    @par !example example_2


    The mixin's `await_transform` intercepts @ref this_coro::environment_tag
    and the fine-grained tag types (@ref this_coro::executor_tag,
    @ref this_coro::stop_token_tag, @ref this_coro::frame_allocator_tag),
    then delegates all other awaitables to your `transform_awaitable`.

    @par Making Your Coroutine an IoAwaitable

    The mixin handles the "inside the coroutine" part—accessing the
    environment. To receive the environment when your coroutine is awaited
    (satisfying @ref IoAwaitable), implement the `await_suspend` overload
    on your coroutine return type:

    @par !example example_3


    @par Thread Safety
    The environment is stored during `await_suspend` and read during
    `co_await this_coro::environment`. These occur on the same logical
    thread of execution, so no synchronization is required.

    @see this_coro::environment, this_coro::executor,
         this_coro::stop_token, this_coro::frame_allocator
    @see io_env
    @see IoAwaitable
*/
template<typename Derived>
class io_awaitable_promise_base
    : public frame_alloc_mixin
{
    io_env const* env_ = nullptr;
    mutable std::coroutine_handle<> cont_{std::noop_coroutine()};
    // The awaiting parent's task object, while it awaits this one;
    // released before the continuation (that parent) is destroyed.
    void* owner_ = nullptr;
    void (*release_owner_)(void*) noexcept = nullptr;

public:
    /** Destroy the promise, destroying an orphaned continuation.

        A continuation is still stored only when the coroutine never
        reached `final_suspend`, because @ref continuation consumes the
        stored handle. Destroying it here is what keeps an abandoned
        coroutine from leaking the trampoline frame that was waiting on it.

        While a parent awaits this coroutine, the task object that owns
        this frame, which lives in the parent, is registered with
        @ref set_owner. It is released first, so destroying the stored
        continuation does not destroy this frame a second time. This
        lets the party whose operation holds a suspended coroutine, such
        as an execution context being torn down, destroy it without
        destroying it twice.

        The continuation is destroyed after this frame has been
        deallocated, so a frame whose memory comes from a resource held
        by an ancestor is freed before that ancestor.

        @par Preconditions
        The derived promise either uses the `operator delete` of
        @ref frame_alloc_mixin, or calls
        `detail::drain_destroy_after_free(ptr, size)` after deallocating
        in its own.
    */
    ~io_awaitable_promise_base()
    {
        if(owner_)
            release_owner_(owner_);
        if(cont_ != std::noop_coroutine())
            detail::destroy_after_free(
                std::exchange(cont_, std::noop_coroutine()), this);
    }

    /** Register the task object that owns this coroutine while awaited.

        Called by an awaiter that owns this coroutine's frame and installs
        its own coroutine as the continuation. If this coroutine is
        destroyed before it completes, @p owner is released first, so
        destroying the continuation does not destroy this frame again.

        @param owner The owning task object. It must stay at the same
            address until @ref clear_owner is called or this coroutine
            is destroyed.
    */
    template<class Owner>
    void set_owner(Owner& owner) noexcept
    {
        owner_         = &owner;
        release_owner_ = [](void* p) noexcept {
            static_cast<Owner*>(p)->release();
        };
    }

    /// Forget the owner registered with @ref set_owner.
    void clear_owner() noexcept
    {
        owner_ = nullptr;
    }

    /// Return true while an awaiting object owns this coroutine.
    bool has_owner() const noexcept
    {
        return owner_ != nullptr;
    }

    //----------------------------------------------------------
    // Continuation support
    //----------------------------------------------------------

    /** Store the continuation to resume on completion.

        Call this from your coroutine type's `await_suspend` overload
        to set up the completion path. The `final_suspend` awaiter
        returns this handle via unconditional symmetric transfer.

        @param cont The continuation to resume on completion.
    */
    void set_continuation(std::coroutine_handle<> cont) noexcept
    {
        cont_ = cont;
    }

    /** Return and consume the stored continuation handle.

        Resets the stored handle to `noop_coroutine()` so the
        destructor does not double-destroy it.

        @return The continuation for symmetric transfer.
    */
    std::coroutine_handle<> continuation() const noexcept
    {
        return std::exchange(cont_, std::noop_coroutine());
    }

    //----------------------------------------------------------
    // Environment support
    //----------------------------------------------------------

    /** Store a pointer to the execution environment.

        Call this from your coroutine type's `await_suspend`
        overload to make the environment available via
        `co_await this_coro::environment`. The pointed-to
        `io_env` must outlive this coroutine.

        @param env The environment to store.
    */
    void set_environment(io_env const* env) noexcept
    {
        env_ = env;
    }

    /** Return the stored execution environment.

        @return The environment.
    */
    io_env const* environment() const noexcept
    {
        BOOST_CAPY_ASSERT(env_);
        return env_;
    }

    /** Transform an awaitable before co_await.

        Override this in your derived promise type to customize how
        awaitables are transformed. The default implementation passes
        the awaitable through unchanged.

        @param a The awaitable expression from `co_await a`.

        @return The transformed awaitable.
    */
    template<typename A>
    decltype(auto) transform_awaitable(A&& a)
    {
        return std::forward<A>(a);
    }

    /** Intercept co_await expressions.

        This function handles @ref this_coro::environment_tag and
        the fine-grained tags (@ref this_coro::executor_tag,
        @ref this_coro::stop_token_tag, @ref this_coro::frame_allocator_tag)
        specially, returning an awaiter that yields the stored value.
        All other awaitables are delegated to @ref transform_awaitable.

        @param t The awaited expression.

        @return An awaiter for the expression.
    */
    template<typename T>
    auto await_transform(T&& t)
    {
        using Tag = std::decay_t<T>;

        if constexpr (std::is_same_v<Tag, this_coro::environment_tag>)
        {
            BOOST_CAPY_ASSERT(env_);
            struct awaiter
            {
                io_env const* env_;
                bool await_ready() const noexcept { return true; }
                void await_suspend(std::coroutine_handle<>) const noexcept { }
                io_env const* await_resume() const noexcept { return env_; }
            };
            return awaiter{env_};
        }
        else if constexpr (std::is_same_v<Tag, this_coro::executor_tag>)
        {
            BOOST_CAPY_ASSERT(env_);
            struct awaiter
            {
                executor_ref executor_;
                bool await_ready() const noexcept { return true; }
                void await_suspend(std::coroutine_handle<>) const noexcept { } // LCOV_EXCL_LINE await_ready() always true, never suspends
                executor_ref await_resume() const noexcept { return executor_; }
            };
            return awaiter{env_->executor};
        }
        else if constexpr (std::is_same_v<Tag, this_coro::stop_token_tag>)
        {
            BOOST_CAPY_ASSERT(env_);
            struct awaiter
            {
                std::stop_token token_;
                bool await_ready() const noexcept { return true; }
                void await_suspend(std::coroutine_handle<>) const noexcept { } // LCOV_EXCL_LINE await_ready() always true, never suspends
                std::stop_token await_resume() const noexcept { return token_; }
            };
            return awaiter{env_->stop_token};
        }
        else if constexpr (std::is_same_v<Tag, this_coro::frame_allocator_tag>)
        {
            BOOST_CAPY_ASSERT(env_);
            struct awaiter
            {
                std::pmr::memory_resource* frame_allocator_;
                bool await_ready() const noexcept { return true; }
                void await_suspend(std::coroutine_handle<>) const noexcept { } // LCOV_EXCL_LINE await_ready() always true, never suspends
                std::pmr::memory_resource* await_resume() const noexcept { return frame_allocator_; }
            };
            return awaiter{env_->frame_allocator};
        }
        else
        {
            return static_cast<Derived*>(this)->transform_awaitable(
                std::forward<T>(t));
        }
    }
};

} // namespace capy
} // namespace boost

#endif

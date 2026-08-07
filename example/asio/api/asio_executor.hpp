//
// Copyright (c) 2026 Steve Gerbino
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//
// Official repository: https://github.com/cppalliance/capy
//

#ifndef CAPY_EXAMPLE_ASIO_EXECUTOR_HPP
#define CAPY_EXAMPLE_ASIO_EXECUTOR_HPP

#include <boost/asio/associated_allocator.hpp>
#include <boost/asio/execution.hpp>
#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/io_context.hpp>

#include <boost/capy/concept/executor.hpp>
#include <boost/capy/continuation.hpp>
#include <boost/capy/ex/frame_allocator.hpp>

#include <coroutine>
#include <memory>
#include <new>
#include <thread>
#include <utility>

namespace net = boost::asio;
namespace capy = boost::capy;

namespace bridge_detail {

//----------------------------------------------------------
//
// exec_frame - synthetic coroutine frame (P4126R0)
//
//----------------------------------------------------------

// The first two members match the coroutine frame layout used by
// MSVC, GCC, and Clang, so from_address on an exec_frame yields a
// handle whose .resume() calls resume(this) and whose .destroy()
// calls destroy(this). Same technique as example/awaitable-sender.
struct exec_frame
{
    void (*resume)(exec_frame*);
    void (*destroy)(exec_frame*);
};

// One node per submitted callable. The node IS the continuation:
// no coroutine frame is created, and the capy executor cannot tell
// the difference. `reserved` stays untouched by us, so the node
// composes with schedulers (e.g. corosio) that use it as their
// post-submission queue link.
template<class F, class CapyExecutor>
struct exec_node : exec_frame
{
    using allocator_type = typename std::allocator_traits<
        net::associated_allocator_t<F>>::
            template rebind_alloc<exec_node>;

    capy::continuation cont;
    CapyExecutor ex;
    allocator_type alloc;
    F fn;

    exec_node(F fn_, CapyExecutor ex_, allocator_type alloc_)
        : exec_frame{&do_resume, &do_destroy}
        , cont{std::coroutine_handle<>::from_address(
              static_cast<void*>(static_cast<exec_frame*>(this)))}
        , ex(std::move(ex_))
        , alloc(std::move(alloc_))
        , fn(std::move(fn_))
    {
    }

    // Deallocate-before-invoke preserves asio's handler allocation
    // guarantee. noexcept: a throwing callable terminates, matching
    // capy's run_async unhandled-exception policy (the capy pump
    // does not expect throwing resumes).
    static void
    do_resume(exec_frame* f) noexcept
    {
        auto* self = static_cast<exec_node*>(f);
        auto ex = std::move(self->ex);
        auto alloc = std::move(self->alloc);
        F fn = std::move(self->fn);
        std::allocator_traits<allocator_type>::destroy(
            alloc, self);
        std::allocator_traits<allocator_type>::deallocate(
            alloc, self, 1);
        std::move(fn)();
        ex.on_work_finished();
    }

    // Reached only if the capy context destroys queued work at
    // shutdown. The callable is dropped, never invoked.
    static void
    do_destroy(exec_frame* f) noexcept
    {
        auto* self = static_cast<exec_node*>(f);
        auto ex = std::move(self->ex);
        auto alloc = std::move(self->alloc);
        std::allocator_traits<allocator_type>::destroy(
            alloc, self);
        std::allocator_traits<allocator_type>::deallocate(
            alloc, self, 1);
        ex.on_work_finished();
    }
};

// Wrap fn in a node and hand it to the capy executor. The work
// pairing (started here, finished in do_resume/do_destroy) keeps
// the capy context alive while a bridged callable is in flight.
template<class CapyExecutor, class F>
void
submit(CapyExecutor const& ex, F fn, bool blocking_never)
{
    using node_type = exec_node<F, CapyExecutor>;
    using traits = std::allocator_traits<
        typename node_type::allocator_type>;

    typename node_type::allocator_type alloc(
        net::get_associated_allocator(fn));
    node_type* p = traits::allocate(alloc, 1);
    try
    {
        ::new(static_cast<void*>(p))
            node_type(std::move(fn), ex, alloc);
    }
    catch(...)
    {
        traits::deallocate(alloc, p, 1);
        throw;
    }

    ex.on_work_started();
    try
    {
        if(blocking_never)
        {
            ex.post(p->cont);
        }
        else
        {
            // dispatch may return the node's own handle (inline case);
            // safe_resume preserves the TLS frame allocator either way.
            capy::safe_resume(ex.dispatch(p->cont));
        }
    }
    catch(...)
    {
        // The Executor concept does not require post/dispatch to be
        // noexcept; unwind the work count and node so a throw here
        // can't leak the node or hang join() on an unbalanced count.
        ex.on_work_finished();
        traits::destroy(alloc, p);
        traits::deallocate(alloc, p, 1);
        throw;
    }
}

} // namespace bridge_detail

//----------------------------------------------------------
//
// asio_executor - asio-shaped executor over a capy executor
//
//----------------------------------------------------------

/** An asio standard executor that submits to a capy executor.

    Satisfies asio's standard (P0443-style) executor requirements,
    so `net::post`, `net::dispatch`, `net::defer`, `net::strand`,
    and asio I/O objects can all target a capy scheduler. Every
    submitted callable travels as a synthetic-frame
    `capy::continuation`; the capy executor runs it like any other
    continuation, so the native capy path is unchanged.

    Obtain instances from @ref bridge_context::get_executor. The
    `context` query returns the bridge's internal `io_context`,
    which hosts asio services (strands, reactors, timers).

    A callable that throws terminates the program.
*/
template<capy::Executor E>
class asio_executor
{
    E ex_;
    net::io_context* io_;
    net::execution::blocking_t blocking_ =
        net::execution::blocking.possibly;
    net::execution::relationship_t relationship_ =
        net::execution::relationship.fork;
    bool tracked_ = false;

public:
    /// Construct from a capy executor and the bridge's io_context.
    asio_executor(E ex, net::io_context& io) noexcept
        : ex_(std::move(ex))
        , io_(&io)
    {
    }

    /// Copy; a tracked copy acquires one unit of capy work.
    asio_executor(asio_executor const& other) noexcept
        : ex_(other.ex_)
        , io_(other.io_)
        , blocking_(other.blocking_)
        , relationship_(other.relationship_)
        , tracked_(other.tracked_)
    {
        if(tracked_)
            ex_.on_work_started();
    }

    /// Move; work ownership transfers, the source becomes untracked.
    asio_executor(asio_executor&& other) noexcept
        : ex_(other.ex_)
        , io_(other.io_)
        , blocking_(other.blocking_)
        , relationship_(other.relationship_)
        , tracked_(std::exchange(other.tracked_, false))
    {
    }

    asio_executor&
    operator=(asio_executor const& other) noexcept
    {
        if(this != &other)
        {
            // Acquire the new unit before releasing the old so the
            // work count never transiently reaches zero.
            if(other.tracked_)
                other.ex_.on_work_started();
            if(tracked_)
                ex_.on_work_finished();
            ex_ = other.ex_;
            io_ = other.io_;
            blocking_ = other.blocking_;
            relationship_ = other.relationship_;
            tracked_ = other.tracked_;
        }
        return *this;
    }

    /// Destroy; a tracked executor releases its unit of capy work.
    ~asio_executor()
    {
        if(tracked_)
            ex_.on_work_finished();
    }

    /** Submit a callable to the capy executor.

        `blocking.never` posts; `blocking.possibly` (the default)
        dispatches, which may invoke inline when already on a capy
        scheduler thread.
    */
    template<class F>
    void
    execute(F&& f) const
    {
        bridge_detail::submit(
            ex_,
            std::decay_t<F>(std::forward<F>(f)),
            blocking_ == net::execution::blocking.never);
    }

    /// Return a copy that posts unconditionally.
    asio_executor
    require(net::execution::blocking_t::never_t) const noexcept
    {
        auto r = *this;
        r.blocking_ = net::execution::blocking.never;
        return r;
    }

    /// Return a copy that may invoke inline on a scheduler thread.
    asio_executor
    require(net::execution::blocking_t::possibly_t) const noexcept
    {
        auto r = *this;
        r.blocking_ = net::execution::blocking.possibly;
        return r;
    }

    /// Return a copy marked as submitting independent work.
    asio_executor
    require(net::execution::relationship_t::fork_t) const noexcept
    {
        auto r = *this;
        r.relationship_ = net::execution::relationship.fork;
        return r;
    }

    /// Return a copy marked as submitting a continuation (hint only).
    asio_executor
    require(net::execution::relationship_t::continuation_t)
        const noexcept
    {
        auto r = *this;
        r.relationship_ = net::execution::relationship.continuation;
        return r;
    }

    /// Return a copy whose existence counts as outstanding capy work.
    asio_executor
    require(net::execution::outstanding_work_t::tracked_t)
        const noexcept
    {
        auto r = *this;
        if(!r.tracked_)
        {
            r.tracked_ = true;
            r.ex_.on_work_started();
        }
        return r;
    }

    /// Return a copy that does not count as outstanding work.
    asio_executor
    require(net::execution::outstanding_work_t::untracked_t)
        const noexcept
    {
        auto r = *this;
        if(r.tracked_)
        {
            r.tracked_ = false;
            r.ex_.on_work_finished();
        }
        return r;
    }

    /// Return the blocking property value.
    net::execution::blocking_t
    query(net::execution::blocking_t) const noexcept
    {
        return blocking_;
    }

    /// Return the relationship property value.
    net::execution::relationship_t
    query(net::execution::relationship_t) const noexcept
    {
        return relationship_;
    }

    /// Return the outstanding_work property value.
    net::execution::outstanding_work_t
    query(net::execution::outstanding_work_t) const noexcept
    {
        if(tracked_)
            return net::execution::outstanding_work.tracked;
        return net::execution::outstanding_work.untracked;
    }

    /// Return the bridge's io_context (hosts asio services).
    net::io_context&
    query(net::execution::context_t) const noexcept
    {
        return *io_;
    }

    /// Return true if both target the same capy executor and context.
    bool
    operator==(asio_executor const& other) const noexcept
    {
        return ex_ == other.ex_ &&
            io_ == other.io_ &&
            blocking_ == other.blocking_ &&
            relationship_ == other.relationship_;
    }
};

//----------------------------------------------------------
//
// bridge_context - owns the asio side of the bridge
//
//----------------------------------------------------------

/** Execution context bridging asio submissions onto a capy executor.

    Owns a real `asio::io_context` so asio services (the strand
    service, reactors, timer queues) live in a documented asio
    context, plus one hidden thread pumping it. All handler
    execution flows to the capy executor; the io_context never runs
    user callables.

    Lifetime: the capy context must outlive this object, and this
    object must outlive every executor and I/O object obtained from
    it.

    @par Example
    @code
    capy::thread_pool pool(4);
    {
        bridge_context ctx(pool.get_executor());
        net::post(ctx.get_executor(), []{ ... });  // runs on pool
    }
    pool.join();
    @endcode
*/
template<capy::Executor E>
class bridge_context
{
    E ex_;
    net::io_context io_;
    net::executor_work_guard<
        net::io_context::executor_type> guard_;
    std::thread pump_;  // no jthread: unavailable on macOS/FreeBSD

public:
    using executor_type = asio_executor<E>;

    /// Construct over a capy executor; starts the pump thread.
    explicit
    bridge_context(E ex)
        : ex_(std::move(ex))
        , guard_(net::make_work_guard(io_))
        , pump_([this]{ io_.run(); })
    {
    }

    /// Destroy the context; drains and joins the pump thread.
    ~bridge_context()
    {
        guard_.reset();
        pump_.join();
    }

    bridge_context(bridge_context const&) = delete;
    bridge_context& operator=(bridge_context const&) = delete;

    /// Return an asio-shaped executor submitting to the capy executor.
    executor_type
    get_executor() noexcept
    {
        return executor_type(ex_, io_);
    }

    /// Return the internal io_context.
    net::io_context&
    io() noexcept
    {
        return io_;
    }
};

#endif

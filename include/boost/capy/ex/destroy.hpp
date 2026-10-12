//
// Copyright (c) 2026 Steve Gerbino
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//
// Official repository: https://github.com/cppalliance/capy
//

#ifndef BOOST_CAPY_EX_DESTROY_HPP
#define BOOST_CAPY_EX_DESTROY_HPP

#include <boost/capy/detail/config.hpp>

#include <coroutine>
#include <cstddef>
#include <cstdint>
#include <new>

namespace boost {
namespace capy {
namespace detail {

// Trivially destructible so it is constant-initialized on every
// toolchain and safe to use from any thread_local destructor. Most
// cascades fit the inline slots; a larger one spills to the heap,
// which the outermost drain releases.
struct destroy_state
{
    static constexpr std::size_t inline_capacity = 16;

    std::coroutine_handle<> local[inline_capacity] = {};
    std::coroutine_handle<>* heap = nullptr;
    std::size_t size = 0;
    std::size_t capacity = inline_capacity;
    bool active = false;

    // An address inside the frame that started a self-driven drain.
    void const* self_frame = nullptr;

    std::coroutine_handle<>* data() noexcept
    {
        return heap ? heap : local;
    }

    // noexcept: a failed allocation during teardown terminates.
    void push(std::coroutine_handle<> h) noexcept
    {
        if(size == capacity)
        {
            auto* bigger = static_cast<std::coroutine_handle<>*>(
                ::operator new(2 * capacity * sizeof(h)));
            auto* old = data();
            for(std::size_t i = 0; i < size; ++i)
                bigger[i] = old[i];
            if(heap)
                ::operator delete(heap);
            heap = bigger;
            capacity *= 2;
        }
        data()[size++] = h;
    }

    // Destroys entries above mark; destroying one may push more.
    void drain_to(std::size_t mark) noexcept
    {
        while(size > mark)
        {
            auto p = data()[--size];
            p.destroy();
        }
    }

    void finish_outermost() noexcept
    {
        active = false;
        if(heap)
        {
            ::operator delete(heap);
            heap = nullptr;
            capacity = inline_capacity;
        }
    }
};

/** Return this thread's teardown state.

    Defined in the library, not inline, so that every module of a
    program shares one instance. An inline definition gives each
    shared library its own copy: a @ref capy::destroy call in one
    would set a flag the promise destructors compiled into another
    never see.

    @return A reference to the calling thread's state.
*/
BOOST_CAPY_DECL
destroy_state&
destroy_state_tls() noexcept;

/** Destroy the parent released by a frame being destroyed.

    Called from a promise destructor, or from code it runs, when the
    frame being destroyed releases the coroutine that awaited it.
    While a @ref capy::destroy is in progress on this thread, the
    parent is destroyed by that call once the frame has been
    completely freed, since that frame's memory may come from a
    resource the parent's chain owns. Otherwise the parent is
    destroyed immediately.

    @note A failed allocation calls `std::terminate`.

    @param p The parent to destroy.
*/
// Runs only for frames destroyed before completing; out of line keeps
// this code out of every coroutine's destroy path.
BOOST_CAPY_NOINLINE BOOST_CAPY_COLD inline void
destroy_parent(std::coroutine_handle<> p) noexcept
{
    auto& s = destroy_state_tls();
    if(s.active)
        s.push(p);
    else
        p.destroy();
}

/** Start collecting parents until a frame is deallocated.

    For a frame whose own `operator delete` calls
    @ref finish_self_drain: when no @ref capy::destroy is in progress
    on this thread, parents released from now on are collected, and
    destroyed once that frame has been deallocated, even when the
    holder destroyed it with a raw `destroy()`. Otherwise the enclosing
    call already collects them, and this does nothing.

    @param frame Any address inside the frame being destroyed.
*/
inline void
begin_self_drain(void const* frame) noexcept
{
    auto& s = destroy_state_tls();
    if(s.active)
        return;
    s.active = true;
    s.self_frame = frame;
}

/** Destroy the parents collected for a frame just deallocated.

    Does nothing unless the frame that occupied `[frame, frame + size)`
    started the drain with @ref begin_self_drain.

    @param frame The address of the frame just deallocated.

    @param size The size of that frame, as passed to `operator delete`.
*/
inline void
finish_self_drain(void const* frame, std::size_t size) noexcept
{
    auto& s = destroy_state_tls();
    if(!s.self_frame)
        return;
    // Only the integer values are compared; the frame is already freed.
    auto const at = reinterpret_cast<std::uintptr_t>(s.self_frame);
    if(at - reinterpret_cast<std::uintptr_t>(frame) >= size)
        return;
    s.self_frame = nullptr;
    // Starting required that no drain was active, so every entry is
    // this drain's.
    s.drain_to(0);
    s.finish_outermost();
}

} // namespace detail

/** Destroy a suspended coroutine and every frame its destruction releases.

    The party that holds a suspended coroutine, such as an operation,
    a queue, a primitive's wait list or an execution context being torn
    down, owns it: it either resumes it or destroys it. Destroying it
    also destroys the chain of coroutines awaiting it, from the bottom
    up. A frame's memory may come from a memory resource owned by an
    ancestor frame, for example the allocator passed to `run_async`
    or `run`, and a frame's by-value parameters are destroyed only
    after its promise. Destroying the parent from the child's promise
    destructor would therefore free that resource while the child's
    parameters and storage are still in use.

    This function destroys `h` and then each parent released by the
    frames it destroyed, so every frame is completely freed before any
    parent it released is destroyed. Destroying a parent may release
    further parents; they are destroyed the same way, iteratively, so
    a long chain does not deepen the stack.

    Holders call this function instead of `h.destroy()`. A raw
    `h.destroy()` made while no call to this function is in progress
    on the thread destroys each parent immediately from the child's
    promise destructor, which is correct only while no frame in the
    chain is allocated from a resource owned by one of its ancestors:
    it is not correct under `run_async(ex, alloc)` or `run(ex, alloc)`.
    A raw `h.destroy()` made inside such a call leaves its parents to
    that call, which destroys them before it returns.

    @par Preconditions
    `h` refers to a coroutine suspended at a suspension point, held
    by the caller, that nothing else will resume or destroy.

    @par Effects
    Destroys `h`, then destroys each coroutine released by the frames
    destroyed, until none remain. A call made while another call on
    the same thread is destroying a frame destroys `h` and the parents
    it releases before returning, and leaves the parents recorded by
    the outer call to that call.

    @par Thread Safety
    The parents being collected are tracked per thread. Calls on
    different threads are independent; destroying a coroutine still
    requires that no other thread resumes or destroys it. The
    per-thread state has no destructor, so calling this function from
    a `thread_local` destructor is safe.

    @note A failed allocation while recording a parent calls
    `std::terminate`.

    @param h The suspended coroutine to destroy.

    @see io_awaitable_promise_base
*/
inline void
destroy(std::coroutine_handle<> h) noexcept
{
    auto& s = detail::destroy_state_tls();
    bool const outermost = !s.active;
    s.active = true;
    // Entries below the mark belong to frames an enclosing call is
    // still destroying; only the parents released under this call
    // have had their frames freed when h.destroy() returns.
    std::size_t const mark = s.size;
    h.destroy();
    s.drain_to(mark);
    if(outermost)
        s.finish_outermost();
}

} // namespace capy
} // namespace boost

#endif

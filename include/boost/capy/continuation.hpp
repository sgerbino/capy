//
// Copyright (c) 2026 Michael Vandeberg
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//
// Official repository: https://github.com/cppalliance/capy
//

#ifndef BOOST_CAPY_CONTINUATION_HPP
#define BOOST_CAPY_CONTINUATION_HPP

#include <boost/capy/detail/config.hpp>

#include <coroutine>

namespace boost {
namespace capy {

class chain_owner;

template<class>
class ready_queue;

/** Names one suspended coroutine so executors can queue it without allocating.

    Wraps a `std::coroutine_handle<>` with the owner of its chain
    segment and a private link, so a @ref ready_queue can hold the
    coroutine with no per-post heap allocation.

    @par Ownership and Lifetime

    A continuation does not own its coroutine. A @ref chain_holder
    does, and refers to the continuation while it does. The object
    lives at the site that embeds it (an I/O awaitable, combinator
    state, or trampoline promise).

    A continuation must have a **stable address** while a holder
    refers to it. It must not be moved, destroyed, or queued in more
    than one queue concurrently.

    @par Copy and Move

    Copying a queued continuation produces a second object that no
    queue links. Copy and move are safe only when no holder refers to
    the original.

    @par Thread Safety

    A single continuation must not be accessed concurrently
    without external synchronization. In practice, the
    creating thread sets `h` and posts a holder of the continuation;
    the executor's worker thread later reads `h` and calls
    `h.resume()`. The executor's internal locking provides
    the necessary synchronization between these two accesses.

    @see @ref chain_holder, @ref ready_queue, @ref Executor,
        @ref executor_ref
*/
class continuation
{
    // Queue link, meaningful only while queued.
    void* reserved_ = nullptr;

    template<class>
    friend class ready_queue;

public:
    /** The coroutine handle to resume.

        Set by the code that creates or reuses the continuation, and read
        by the executor when it dequeues it.
    */
    std::coroutine_handle<> h;

    /** The owner of the chain segment the coroutine belongs to.

        Set from the coroutine's @ref await_context when it is parked or
        queued. A holder that gives the coroutine up passes this
        continuation to @ref abandon, which unwinds the segment from its
        owner. Null for a coroutine with no owner, such as an owner's own
        continuation, which @ref abandon destroys directly.
    */
    chain_owner* owner = nullptr;

    /// Construct a continuation that names no coroutine.
    continuation() = default;

    /** Construct a continuation naming a coroutine.

        @param handle The coroutine.

        @param segment_owner The owner of the coroutine's chain segment,
            or null if it has none.
    */
    explicit continuation(
        std::coroutine_handle<> handle,
        chain_owner* segment_owner = nullptr) noexcept
        : h(handle)
        , owner(segment_owner)
    {
    }
};

} // namespace capy
} // namespace boost

#endif

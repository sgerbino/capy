//
// Copyright (c) 2026 Steve Gerbino
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//
// Official repository: https://github.com/cppalliance/capy
//

#ifndef BOOST_CAPY_EX_CHAIN_HOLDER_HPP
#define BOOST_CAPY_EX_CHAIN_HOLDER_HPP

#include <boost/capy/detail/config.hpp>
#include <boost/capy/continuation.hpp>

#include <type_traits>
#include <utility>

namespace boost {
namespace capy {

/** Give up a suspended coroutine for good.

    Called by the holder of a suspended coroutine that will never resume
    it, such as an execution context, strand or primitive being torn
    down. The coroutine's segment is unwound from its owner: on the
    owner's executor, from the top frame down to this coroutine, without
    any frame delivering a result. Ordinary cancellation does not use
    this; it resumes the coroutine with an error instead.

    A continuation with no owner, such as an owner's own continuation,
    is destroyed directly.

    A @ref chain_holder calls this from its destructor. Call it directly
    only for a coroutine no holder can represent, such as one parked in
    a list inside its own frame.

    @par Preconditions
    `c` refers to a suspended coroutine that the caller holds and that
    nothing else will resume or abandon. If `c` has no owner, nothing
    awaits the coroutine: destroying its frame alone would leave any
    awaiting frame holding a destroyed one.

    @par Postconditions
    The caller must not touch the coroutine, or anything in its frame,
    again. The frames may already be destroyed when this returns, if the
    owner's executor destroys posted work at once.

    @note The unwind runs on the owner's executor, which may belong to
    another execution context than the caller's, and may run after the
    caller's context is destroyed. A chain that holds objects of the
    caller's context must therefore have finished before that context
    is torn down.

    @note Each frame is destroyed from the destructor of the frame
    awaiting it, so the unwind uses stack in proportion to how deeply
    the chain's coroutines are nested.

    @param c The continuation of the coroutine given up.
*/
BOOST_CAPY_DECL
void
abandon(continuation& c) noexcept;

/** Owns the right to resume or give up one suspended coroutine.

    A suspended coroutine has exactly one holder, the code that will
    eventually resume it. An executor that drops work without resuming
    it would otherwise leak the whole chain with no report. This type
    makes the correct outcome the default: destroying a `chain_holder`
    that still owns a coroutine gives the coroutine up with
    @ref abandon.

    The referenced @ref continuation stays where it is, usually in the
    coroutine's own frame or awaitable, so posting allocates nothing.

    @par Thread Safety
    Distinct objects: Safe.
    Shared objects: Unsafe.

    @par Example
    @code
    void post(chain_holder&& h) const
    {
        std::lock_guard lock(ctx_->mutex);
        if(ctx_->closed)
            return;  // `h` stays with the caller and abandons
        ctx_->queue.push(std::move(h));
    }
    @endcode

    @note A holder stored inside the frame it owns cannot start the
    unwind, since only the unwind would destroy that frame. Keep owning
    holders outside the chain they own.

    @note Do not let a holder die while a lock is held that the unwind
    may need: abandoning can post, and can run frame destructors at
    once. Declare the holder before the lock so it dies after it.

    @see abandon, ready_queue
*/
class chain_holder
{
    continuation* c_ = nullptr;

public:
    /** Destroy the holder.

        Gives up the coroutine if this object still owns one.
    */
    ~chain_holder()
    {
        if(c_)
            capy::abandon(*c_);
    }

    /// Construct a holder that owns nothing.
    chain_holder() = default;

    /** Construct a holder that takes ownership of a suspended coroutine.

        @par Preconditions
        The caller holds the coroutine `c` refers to, and `c` stays at a
        stable address until the coroutine is resumed or given up.

        @param c The continuation of the coroutine.
    */
    explicit chain_holder(continuation& c) noexcept
        : c_(&c)
    {
    }

    /** Construct a holder by taking ownership from another.

        @param other The holder to take from; it owns nothing afterward.
    */
    chain_holder(chain_holder&& other) noexcept
        : c_(std::exchange(other.c_, nullptr))
    {
    }

    chain_holder(chain_holder const&) = delete;
    chain_holder& operator=(chain_holder const&) = delete;

    /** Take ownership from another holder.

        Gives up any coroutine this object already owns.

        @param other The holder to take from; it owns nothing afterward.

        @return `*this`.
    */
    chain_holder& operator=(chain_holder&& other) noexcept
    {
        // The abandon can run frame destructors, so finish first.
        chain_holder doomed(std::move(*this));
        c_ = std::exchange(other.c_, nullptr);
        return *this;
    }

    /// Check whether this object owns a coroutine.
    explicit operator bool() const noexcept
    {
        return c_ != nullptr;
    }

    /** Give up ownership without abandoning.

        Use this to resume the coroutine, or to pass it where no C++
        object can hold it, such as to a kernel queue or a C callback
        API. Rebuild a holder with the returned continuation when it
        comes back.

        @return The continuation, or null if this object owned nothing.
    */
    continuation* release() noexcept
    {
        return std::exchange(c_, nullptr);
    }

    /** Pass ownership across a call that takes a raw continuation.

        A holder passed by reference through an opaque call forces the
        caller to reload and test it afterward. This calls `f` with the
        raw continuation and clears the holder once `f` returns, so the
        compiler can see nothing is left to test. If `f` throws, the
        holder still owns the coroutine, so a failed post leaves the
        caller holding it.

        @par Preconditions
        This object owns a coroutine, and does not live in storage that
        `f` can end. A temporary or a local variable qualifies.

        @par Exception Safety
        Strong guarantee.

        @param f A callable invoked with the `continuation&`. Unless it
            throws, it now owns the coroutine.

        @return The result of `f`.
    */
    template<class F>
    decltype(auto)
    transfer(F&& f)
    {
        // No catch: it would stop await_suspend inlining into the
        // coroutine, which costs more than the transfer itself.
        using result = decltype(std::forward<F>(f)(*c_));
        if constexpr(std::is_void_v<result>)
        {
            std::forward<F>(f)(*c_);
            c_ = nullptr;
        }
        else
        {
            result r = std::forward<F>(f)(*c_);
            c_ = nullptr;
            return r;
        }
    }
};

namespace detail {

// Receives what chain_holder::transfer sent: `f` gets a holder, and a
// throw hands the coroutine back to the sender without abandoning it.
template<class F>
decltype(auto)
adopt_chain(continuation& c, F&& f)
{
    chain_holder h(c);
    try
    {
        return std::forward<F>(f)(h);
    }
    catch(...)
    {
        h.release();
        throw;
    }
}

} // namespace detail

} // namespace capy
} // namespace boost

#endif

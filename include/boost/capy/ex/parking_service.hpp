//
// Copyright (c) 2026 Steve Gerbino
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//
// Official repository: https://github.com/cppalliance/capy
//

#ifndef BOOST_CAPY_EX_PARKING_SERVICE_HPP
#define BOOST_CAPY_EX_PARKING_SERVICE_HPP

#include <boost/capy/detail/config.hpp>
#include <boost/capy/continuation.hpp>
#include <boost/capy/ex/chain_holder.hpp>
#include <boost/capy/ex/execution_context.hpp>

#include <mutex>
#include <tuple>

namespace boost {
namespace capy {

/** A service base that gives up its parked coroutines at shutdown.

    A service that parks coroutines cannot rely on its destructor to
    give them up. When two services hold chains that own objects of the
    other, whichever is destroyed first leaves the other's unwind
    calling into freed memory. The give-up has to happen in the phase
    before any service is destroyed, and this base runs it there, so
    the derived service writes no teardown step for its waiters.

    A @ref waiter lives in the awaitable, in the coroutine's frame.
    Whoever calls @ref unpark and receives a non-empty holder owns the
    coroutine, which also settles a race between a completion and a
    cancellation: one of them gets the holder.

    @par Thread Safety
    Distinct objects: Safe.
    Shared objects: Safe.

    @par Example
    @code
    struct my_service : parking_service
    {
        explicit my_service(execution_context&) {}

        void complete(waiter& w, executor_ref ex)
        {
            if(auto h = unpark(w))
                ex.post(std::move(h));
        }
    };
    @endcode
*/
class parking_service : public execution_context::service
{
public:
    /// The place a coroutine holds in the service's list.
    class waiter
    {
        waiter* prev_ = nullptr;
        waiter* next_ = nullptr;
        bool parked_ = false;

        friend class parking_service;

    public:
        /// The parked coroutine, with its owner set.
        continuation cont;
    };

private:
    std::mutex mutex_;
    waiter* head_ = nullptr;
    bool closed_ = false;

    void
    unlink(waiter& w) noexcept
    {
        if(w.prev_)
            w.prev_->next_ = w.next_;
        else
            head_ = w.next_;
        if(w.next_)
            w.next_->prev_ = w.prev_;
        w.prev_ = w.next_ = nullptr;
        w.parked_ = false;
    }

protected:
    parking_service() = default;

    /** Add a suspended coroutine to the service's list.

        After shutdown has begun the coroutine is given up at once.

        @par Postconditions
        The caller must not touch `w` or the frame holding it, as
        another thread or an immediate give-up may already have ended
        them.

        @param w The waiter, with `w.cont` set.
    */
    void
    park(waiter& w) noexcept
    {
        // Declared before the lock so a refused coroutine unwinds
        // after it is released.
        chain_holder refused;
        std::lock_guard<std::mutex> lock(mutex_);
        if(closed_)
        {
            refused = chain_holder(w.cont);
            return;
        }
        w.next_ = head_;
        if(head_)
            head_->prev_ = &w;
        head_ = &w;
        w.parked_ = true;
    }

    /** Claim a parked coroutine.

        @param w The waiter to claim.

        @return A holder of the coroutine, or an empty holder if it was
            already claimed or shutdown took it first.
    */
    chain_holder
    unpark(waiter& w) noexcept
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if(!w.parked_)
            return {};
        unlink(w);
        return chain_holder(w.cont);
    }

    /** Called for each waiter shutdown gives up, before the give-up.

        Override to release what the service holds for the waiter, such
        as a work count or a stop callback. The frame is still alive.

        @param w The waiter about to be given up.
    */
    virtual
    void
    on_give_up(waiter& w) noexcept
    {
        std::ignore = w;
    }

    /** Give up every parked coroutine.

        Runs in the phase before any service is destroyed. Each waiter
        is taken under the lock and given up outside it, because an
        unwind can destroy other waiters or re-enter the service.
    */
    void
    shutdown() override
    {
        for(;;)
        {
            chain_holder h;
            waiter* w;
            {
                std::lock_guard<std::mutex> lock(mutex_);
                closed_ = true;
                w = head_;
                if(!w)
                    return;
                unlink(*w);
                h = chain_holder(w->cont);
            }
            on_give_up(*w);
        }
    }
};

} // namespace capy
} // namespace boost

#endif

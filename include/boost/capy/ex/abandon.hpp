//
// Copyright (c) 2026 Steve Gerbino
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//
// Official repository: https://github.com/cppalliance/capy
//

#ifndef BOOST_CAPY_EX_ABANDON_HPP
#define BOOST_CAPY_EX_ABANDON_HPP

#include <boost/capy/detail/config.hpp>
#include <boost/capy/continuation.hpp>
#include <boost/capy/ex/chain_holder.hpp>
#include <boost/capy/ex/executor_ref.hpp>

#include <atomic>
#include <coroutine>

namespace boost {
namespace capy {

/** The owner of one segment of a coroutine chain.

    A segment is a run of coroutines started together: the task
    `run_async` launches and everything it awaits, a task `run` starts
    on another executor, or one branch of `when_all` or `when_any`. Its
    owner is the coroutine that started it, suspended above the
    segment's top frame until that frame completes. Every coroutine in
    the segment reaches the owner through its @ref await_context.

    When the holder of a suspended coroutine in the segment gives it up
    with @ref abandon, the owner posts its own continuation to its
    executor. There it unwinds the segment from the top: it destroys
    the frame it awaits, which destroys the frames below it in turn, and
    finishes without delivering a result. An executor that destroys the
    posted continuation instead, as a closed one does, unwinds the
    segment the same way.

    @par Thread Safety
    @ref abandon may be called from any thread. Only one holder can
    abandon a segment, since it is suspended at one point; a repeated
    call before the unwind runs does nothing.
*/
class chain_owner
{
    std::atomic<bool> abandoned_{false};
    continuation self_;
    executor_ref ex_;

public:
    /** Bind the owner to its coroutine and executor.

        Called by the owner before it starts the segment.

        @param self The owner's coroutine, resumed or destroyed to
            unwind the segment.

        @param ex The executor the segment runs on.
    */
    void bind(std::coroutine_handle<> self, executor_ref ex) noexcept
    {
        self_.h = self;
        ex_ = ex;
    }

    /// Return true once the segment has been abandoned.
    bool abandoned() const noexcept
    {
        return abandoned_.load(std::memory_order_acquire);
    }

    /** Unwind the segment on the owner's executor.

        Does nothing if the segment was already abandoned. Otherwise
        posts the owner's continuation; nothing of the owner is touched
        afterward, since the post may unwind and free it at once.

        @note A post that throws calls `std::terminate`.
    */
    void abandon() noexcept
    {
        if(abandoned_.exchange(true, std::memory_order_acq_rel))
            return;
        ex_.post(chain_holder(self_));
    }
};

} // namespace capy
} // namespace boost

#endif

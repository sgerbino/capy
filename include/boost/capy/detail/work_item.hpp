//
// Copyright (c) 2026 Steve Gerbino
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//
// Official repository: https://github.com/cppalliance/capy
//

#ifndef BOOST_CAPY_DETAIL_WORK_ITEM_HPP
#define BOOST_CAPY_DETAIL_WORK_ITEM_HPP

#include <boost/capy/detail/intrusive.hpp>

namespace boost {
namespace capy {

/** Base class for work items that can be enqueued without allocation.

    Derive from this class to create work items that embed the
    intrusive queue node directly in the object. This eliminates
    the per-operation heap allocation that @ref executor_type::post
    requires when wrapping a raw coroutine handle.

    The derived class implements @ref execute, which the execution
    context calls when the work item reaches the front of the queue.
    The work item must remain alive from the call to `enqueue`
    until `execute` returns.

    @par Thread Safety
    Not thread-safe. The execution context serializes access.

    @par Example
    @code
    struct my_awaitable : work_item
    {
        std::coroutine_handle<> h_;

        void execute() noexcept override
        {
            h_.resume();
        }
    };
    @endcode

    @see thread_pool::executor_type::enqueue
*/
struct work_item : detail::intrusive_queue<work_item>::node
{
    /// Run this work item. Called by the execution context.
    virtual void execute() noexcept = 0;

protected:
    ~work_item() = default;
};

} // capy
} // boost

#endif

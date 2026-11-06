//
// Copyright (c) 2026 Steve Gerbino
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//
// Official repository: https://github.com/cppalliance/capy
//

#ifndef CAPY_EXAMPLE_RESUME_OR_ABANDON_HPP
#define CAPY_EXAMPLE_RESUME_OR_ABANDON_HPP

#include <boost/capy/continuation.hpp>
#include <boost/capy/ex/chain_holder.hpp>
#include <boost/capy/ex/frame_allocator.hpp>

// Resumes its coroutine when Asio runs it; gives it up when Asio
// destroys it unrun, as when its io_context is destroyed.
class resume_or_abandon
{
    boost::capy::chain_holder h_;

public:
    explicit resume_or_abandon(boost::capy::continuation& c) noexcept
        : h_(c)
    {
    }

    // Leave the coroutine to the caller, as when the post that was to
    // carry it throws. Returns false if this handler was moved from.
    bool release() noexcept
    {
        return h_.release() != nullptr;
    }

    void operator()()
    {
        boost::capy::safe_resume(h_.release()->h);
    }
};

#endif

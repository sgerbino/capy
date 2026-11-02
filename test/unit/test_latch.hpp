//
// Copyright (c) 2026 Steve Gerbino
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//
// Official repository: https://github.com/cppalliance/capy
//

#ifndef BOOST_CAPY_TEST_UNIT_TEST_LATCH_HPP
#define BOOST_CAPY_TEST_UNIT_TEST_LATCH_HPP

#include <cstddef>
#include <latch>
#include <tuple>

namespace boost {
namespace capy {

/* A std::latch that ThreadSanitizer can follow.

   When std::latch::wait blocks, libstdc++ 16 reloads the counter inside
   __wait_impl in libstdc++.so, which TSan does not instrument, and
   skips the acquire load in the header (_M_on_wake in
   bits/atomic_wait.h). TSan then misses the synchronization with
   count_down's release, and reports a race between writes made before
   count_down and reads made after wait, although the latch orders them.
   A wait that finds the count already zero takes the instrumented path,
   so the report comes and goes with timing. GCC 15 re-checks the
   counter in the header and is not affected.

   try_wait after wait loads the counter with acquire, in code TSan sees,
   and reads the zero that the last count_down released: that restores
   the happens-before. Drop this class once libstdc++ annotates the wait
   or makes that load visible to TSan. */
class test_latch
{
    std::latch l_;

public:
    explicit test_latch(std::ptrdiff_t expected)
        : l_(expected)
    {
    }

    void count_down(std::ptrdiff_t n = 1)
    {
        l_.count_down(n);
    }

    void wait() const
    {
        l_.wait();
        std::ignore = l_.try_wait();
    }
};

} // namespace capy
} // namespace boost

#endif

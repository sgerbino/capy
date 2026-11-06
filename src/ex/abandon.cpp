//
// Copyright (c) 2026 Steve Gerbino
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//
// Official repository: https://github.com/cppalliance/capy
//

#include <boost/capy/ex/abandon.hpp>

namespace boost {
namespace capy {

void
abandon(continuation& c) noexcept
{
    if(c.owner)
        c.owner->abandon();
    else if(c.h && c.h != std::noop_coroutine())
        c.h.destroy();
}

} // namespace capy
} // namespace boost

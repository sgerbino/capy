//
// Copyright (c) 2026 Steve Gerbino
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//
// Official repository: https://github.com/cppalliance/capy
//

#include <boost/capy/ex/destroy.hpp>

namespace boost {
namespace capy {
namespace detail {

destroy_state&
destroy_state_tls() noexcept
{
#if defined(__cpp_constinit)
    static thread_local constinit destroy_state s;
#else
    static thread_local destroy_state s;
#endif
    return s;
}

} // namespace detail
} // namespace capy
} // namespace boost

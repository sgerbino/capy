//
// Copyright (c) 2026 Michael Vandeberg
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//
// Official repository: https://github.com/cppalliance/capy
//

#ifndef BOOST_CAPY_DETAIL_QUITTER_STOPPED_HPP
#define BOOST_CAPY_DETAIL_QUITTER_STOPPED_HPP

#include <boost/capy/detail/config.hpp>
#include <boost/capy/error.hpp>

#include <system_error>

/* What run_async reports when it started a quitter<T> whose T cannot
   hold canceled (void, or a plain T) and the quitter stopped. It is a
   std::system_error with canceled, so an error handler catches it as
   one. Its own type lets run_async's default_handler drop the stop,
   while it still terminates on any other exception, a real ECANCELED
   included.
   The type is visible so that catch still matches a stop thrown from
   another shared library in a hidden-visibility build. */

namespace boost {
namespace capy {
namespace detail {

struct BOOST_CAPY_SYMBOL_VISIBLE
    quitter_stopped
    : std::system_error
{
    quitter_stopped()
        : std::system_error(make_error_code(error::canceled))
    {
    }
};

} // namespace detail
} // namespace capy
} // namespace boost

#endif

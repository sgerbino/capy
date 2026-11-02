//
// Copyright (c) 2026 Steve Gerbino
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//
// Official repository: https://github.com/cppalliance/capy
//

// Compiled with exceptions disabled, never run. A quitter<> or a
// quitter of a plain T has no stopped value without exceptions, so
// joining one must not instantiate it.

#include <boost/capy/quitter.hpp>
#include <boost/capy/task.hpp>

#include <system_error>
#include <tuple>

namespace boost {
namespace capy {
namespace {

quitter<> joined_void()
{
    co_return;
}

quitter<int> joined_int()
{
    co_return 1;
}

quitter<std::error_code> joins()
{
    co_await joined_void();
    std::ignore = co_await joined_int();
    co_return std::error_code{};
}

[[maybe_unused]] task<> awaits()
{
    std::ignore = co_await joins();
}

} // namespace
} // capy
} // boost

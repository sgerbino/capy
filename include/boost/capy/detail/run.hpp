//
// Copyright (c) 2025 Vinnie Falco (vinnie.falco@gmail.com)
// Copyright (c) 2026 Michael Vandeberg
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//
// Official repository: https://github.com/cppalliance/capy
//

#ifndef BOOST_CAPY_DETAIL_RUN_HPP
#define BOOST_CAPY_DETAIL_RUN_HPP

#include <boost/capy/detail/config.hpp>
#include <boost/capy/detail/frame_memory_resource.hpp>

#include <concepts>
#include <coroutine>
#include <cstddef>
#include <memory_resource>

namespace boost {
namespace capy {
namespace detail {

// Reaches the launch hooks of a runnable that a stop ends by
// abandonment (quitter). They are private: only capy's launchers drive
// a quitter's handle, and every other caller awaits it.
struct launch_access
{
    // What the runnable delivers instead of its own result.
    template<class Promise>
    static auto stopped_result() -> decltype(Promise::stopped_result())
    {
        return Promise::stopped_result();
    }

    // The handle a launcher resumes to start the runnable.
    template<class Promise>
    static auto launch_handle(Promise& p) -> decltype(p.launch_handle())
    {
        return p.launch_handle();
    }
};

template<class Task>
concept stops_by_abandonment = requires(typename Task::promise_type& p) {
    launch_access::stopped_result<typename Task::promise_type>();
    { launch_access::launch_handle(p) } ->
        std::convertible_to<std::coroutine_handle<>>;
};

/// Concept for standard Allocator types.
template<class A>
concept Allocator = requires(A a, std::size_t n) {
    typename A::value_type;
    { a.allocate(n) } -> std::same_as<typename A::value_type*>;
};

/// Specialization for memory_resource* - stores pointer directly.
template<>
class frame_memory_resource<std::pmr::memory_resource*>
{
    std::pmr::memory_resource* mr_;

public:
    explicit frame_memory_resource(std::pmr::memory_resource* mr) noexcept
        : mr_(mr)
    {
    }

    std::pmr::memory_resource* get() noexcept { return mr_; }
};

/// Specialization for void - no allocator (empty).
template<>
class frame_memory_resource<void>
{
public:
    frame_memory_resource() = default;

    std::pmr::memory_resource* get() noexcept { return nullptr; }
};

} // namespace detail
} // namespace capy
} // namespace boost

#endif

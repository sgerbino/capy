//
// Copyright (c) 2026 Steve Gerbino
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//
// Official repository: https://github.com/cppalliance/capy
//

// Temporary CI probe: where does MSVC lose the identity of an
// rvalue-reference await result? Not for merge.

#include <boost/capy/quitter.hpp>
#include <boost/capy/task.hpp>
#include <boost/capy/test/run_blocking.hpp>

#include "test_helpers.hpp"

#include <queue>
#include <stop_token>
#include <system_error>
#include <tuple>
#include <utility>

#include "test_suite.hpp"

namespace boost {
namespace capy {

namespace {

// Mirrors quitter's transform_awaiter::await_resume: a held pointer
// returned through static_cast under nested if constexpr, from a
// decltype(auto) function with a second return statement.
struct deduced_fwd
{
    std::error_code* p;
    bool held;

    std::error_code&& direct() noexcept { return std::move(*p); }

    decltype(auto) get() noexcept
    {
        if constexpr (true)
        {
            if(held)
            {
                if constexpr (true)
                    return static_cast<std::error_code&&>(*p);
            }
        }
        return direct();
    }
};

struct explicit_fwd
{
    std::error_code* p;
    bool held;

    std::error_code&& direct() noexcept { return std::move(*p); }

    std::error_code&& get() noexcept
    {
        if constexpr (true)
        {
            if(held)
            {
                if constexpr (true)
                    return static_cast<std::error_code&&>(*p);
            }
        }
        return direct();
    }
};

template<class Fwd, bool Ready>
struct fwd_awaitable
{
    Fwd f;

    bool await_ready() const noexcept { return Ready; }

    std::coroutine_handle<>
    await_suspend(std::coroutine_handle<> h, await_context const*) noexcept
    {
        return h;
    }

    decltype(auto) await_resume() noexcept { return f.get(); }
};

// Completes by returning the handle, optionally after a stop.
template<bool Ready>
struct rref_awaitable
{
    std::stop_source* ss;
    std::error_code& ec;

    bool await_ready() noexcept
    {
        if(Ready && ss)
            ss->request_stop();
        return Ready;
    }

    std::coroutine_handle<>
    await_suspend(std::coroutine_handle<> h, await_context const*) noexcept
    {
        if(ss)
            ss->request_stop();
        return h;
    }

    std::error_code&& await_resume() noexcept
    {
        return std::move(ec);
    }
};

template<bool Ready>
quitter<results<>>
quitter_body(std::stop_source* ss, bool& same)
{
    std::error_code ec;
    std::error_code&& r = co_await rref_awaitable<Ready>{ss, ec};
    same = &r == &ec;
    co_return results<>{};
}

} // namespace

struct msvc_probe_test
{
    template<class Fwd>
    static bool
    plain(bool held)
    {
        std::error_code ec;
        Fwd f{&ec, held};
        std::error_code&& r = f.get();
        return &r == &ec;
    }

    template<class Fwd, bool Ready>
    static bool
    in_task(bool held)
    {
        bool same = false;
        auto coro = [&]() -> task<>
        {
            std::error_code ec;
            std::error_code&& r =
                co_await fwd_awaitable<Fwd, Ready>{Fwd{&ec, held}};
            same = &r == &ec;
        };
        test::run_blocking()(coro());
        return same;
    }

    template<bool Ready>
    static bool
    in_quitter(bool stop)
    {
        bool same = false;
        std::stop_source ss;
        std::queue<continuation> q;
        queuing_executor ex(q);
        auto outer = [&]() -> task<>
        {
            std::ignore = co_await quitter_body<Ready>(
                stop ? &ss : nullptr, same);
        };
        run_async(ex, ss.get_token())(outer());
        pump(q);
        return same;
    }

    void
    run()
    {
        // P1: no coroutines.
        BOOST_TEST(plain<deduced_fwd>(true));
        BOOST_TEST(plain<deduced_fwd>(false));
        BOOST_TEST(plain<explicit_fwd>(true));
        BOOST_TEST(plain<explicit_fwd>(false));

        // P2: the same forwarding as a task's awaitable.
        BOOST_TEST((in_task<deduced_fwd, true>(true)));
        BOOST_TEST((in_task<deduced_fwd, false>(true)));
        BOOST_TEST((in_task<explicit_fwd, true>(true)));
        BOOST_TEST((in_task<explicit_fwd, false>(true)));

        // P3: quitter without a stop (no early hold), and with one.
        BOOST_TEST(in_quitter<true>(false));
        BOOST_TEST(in_quitter<false>(false));
        BOOST_TEST(in_quitter<true>(true));
        BOOST_TEST(in_quitter<false>(true));
    }
};

TEST_SUITE(msvc_probe_test, "boost.capy.msvc_probe");

} // namespace capy
} // namespace boost

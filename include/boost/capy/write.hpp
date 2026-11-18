//
// Copyright (c) 2025 Vinnie Falco (vinnie.falco@gmail.com)
// Copyright (c) 2026 Michael Vandeberg
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//
// Official repository: https://github.com/cppalliance/capy
//

#ifndef BOOST_CAPY_WRITE_HPP
#define BOOST_CAPY_WRITE_HPP

#include <boost/capy/detail/config.hpp>
#include <boost/capy/results.hpp>
#include <boost/capy/task.hpp>
#include <boost/capy/buffers.hpp>
#include <boost/capy/detail/consuming_buffers.hpp>
#include <boost/capy/concept/context_awaitable.hpp>
#include <boost/capy/concept/decomposes_to.hpp>
#include <boost/capy/concept/write_stream.hpp>
#include <system_error>

#include <cstddef>

namespace boost {
namespace capy {

namespace detail {

// A stream whose `write` drains a whole buffer sequence in one
// operation, with the result the loop would produce.
template<class S, class CB>
concept stream_writes_all = requires(S& s, CB const& b) {
    { s.write(b) } -> ContextAwaitable;
    requires awaitable_decomposes_to<
        decltype(s.write(b)), std::error_code, std::size_t>;
};

template <WriteStream S, ConstBufferSequence CB>
task<results<std::size_t>> write_loop(S& stream, CB buffers)
{
    detail::consuming_buffers consuming(buffers);
    std::size_t const total_size = buffer_size(buffers);
    std::size_t total_written = 0;

    while(total_written < total_size)
    {
        auto [ec, n] = co_await stream.write_some(consuming.data());
        consuming.consume(n);
        total_written += n;
        // A contingency that still completed the transfer is a success:
        // report it only when not all bytes were written.
        if(ec && total_written < total_size)
            co_return {ec, total_written};
    }

    co_return {std::error_code(), total_written};
}

} // namespace detail

/** Write an entire buffer sequence to a stream.

    @par Await-effects

    Writes the contents of `buffers` to `stream` via awaiting
    `stream.write_some` with consecutive portions of data from `buffers`
    until:

    @li either the full content of @c buffers is processed,
    @li or a contingency in `stream.write_some` occurs.

    If `buffer_size(buffers) == 0` then no awaiting `stream.write_some`
    is performed. This is not a contingency.


    @par Await-returns

    An object of type `results<std::size_t>` destructuring as `[ec, n]`.

    Upon a contingency, `n` represents the number of bytes written
    so far.

    Otherwise `n` represents the number of bytes written.

    Contingencies:

    @li The first contingency reported from awaiting @c stream.write_some
    while not all bytes have been written. A contingency that accompanies
    the write which transfers the last bytes is not reported: a completed
    transfer is a success.

    Notable conditions:

    @li @c cond::canceled — Operation was cancelled,
    @li @c std::errc::broken_pipe — Peer closed connection.


    @par Await-postcondition

    If `n == buffer_size(buffers)` the transfer completed and `ec` is
    success; otherwise `ec` is set.


    @param stream The stream to write to. If the lifetime of `stream` ends
    before the coroutine finishes, the behavior is undefined.

    @param buffers The buffer sequence to write. If the lifetime of the buffer
    sequence represented by `buffers` ends
    before the coroutine finishes, the behavior is undefined.

    @return An awaitable yielding `results<std::size_t>` whose second
    element is the number of bytes written.

    @par Remarks

    Supports _ContextAwaitable cancellation_.

    A stream may have a member `write( buffers )` that writes the
    whole sequence in one operation. That operation then replaces
    the loop and spares a coroutine frame. Its result must follow
    the rules above. Otherwise the awaitable is a `task`.

    @par Example

    @par !example example


    @see @ref WriteStream, @ref ConstBufferSequence, @ref ContextAwaitable, @ref results, @ref cond
*/
template <WriteStream S, ConstBufferSequence CB>
auto write(S& stream, CB buffers)
{
    if constexpr (detail::stream_writes_all<S, CB>)
        return stream.write(buffers);
    else
        return detail::write_loop(stream, std::move(buffers));
}

} // namespace capy
} // namespace boost

#endif

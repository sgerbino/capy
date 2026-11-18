//
// Copyright (c) 2025 Vinnie Falco (vinnie.falco@gmail.com)
// Copyright (c) 2026 Michael Vandeberg
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//
// Official repository: https://github.com/cppalliance/capy
//

#ifndef BOOST_CAPY_READ_HPP
#define BOOST_CAPY_READ_HPP

#include <boost/capy/detail/config.hpp>
#include <boost/capy/cond.hpp>
#include <boost/capy/results.hpp>
#include <boost/capy/task.hpp>
#include <boost/capy/buffers.hpp>
#include <boost/capy/detail/consuming_buffers.hpp>
#include <boost/capy/concept/context_awaitable.hpp>
#include <boost/capy/concept/decomposes_to.hpp>
#include <boost/capy/concept/read_stream.hpp>

#include <algorithm>
#include <cstddef>

namespace boost {
namespace capy {

namespace detail {

// A stream whose `read` fills a whole buffer sequence in one
// operation, with the result the loop would produce.
template<class S, class MB>
concept stream_reads_all = requires(S& s, MB const& b) {
    { s.read(b) } -> ContextAwaitable;
    requires awaitable_decomposes_to<
        decltype(s.read(b)), std::error_code, std::size_t>;
};

template <typename S, typename MB>
task<results<std::size_t>>
read_loop(S& stream, MB buffers)
{
    detail::consuming_buffers consuming(buffers);
    std::size_t const total_size = buffer_size(buffers);
    std::size_t total_read = 0;

    while(total_read < total_size)
    {
        auto [ec, n] = co_await stream.read_some(consuming.data());
        consuming.consume(n);
        total_read += n;
        // A contingency that still completed the transfer is a success:
        // report it only when the buffer was not filled.
        if(ec && total_read < total_size)
            co_return {ec, total_read};
    }

    co_return {std::error_code(), total_read};
}

} // namespace detail

/** Read data from a stream until the buffer sequence is full.

    @par Await-effects

    Reads data from `stream` via awaiting `stream.read_some` repeatedly
    until:

    @li either the entire buffer sequence  @c buffers is filled,
    @li or a contingency occurs on `stream.read_some`.

    If `buffer_size(buffers) == 0` then no awaiting `stream.read_some`
    is performed. This is not a contingency.

    @par Await-returns
    An object of type `results<std::size_t>` destructuring as `[ec, n]`.

    Upon a contingency, `n` represents the number of bytes read so far,
    inclusive of the last partial read.

    Contingencies:

    @li The first contingency reported from awaiting @c stream.read_some
        while `buffers` is not yet filled. A contingency that accompanies
        the read which fills `buffers` is not reported: a completed
        transfer is a success.

    Notable conditions:

    @li @c cond::canceled — Operation was cancelled,
    @li @c cond::eof — Stream reached end before @c buffers was filled.

    @par Await-postcondition
    If `n == buffer_size(buffers)` the transfer completed and `ec` is
    success; otherwise `ec` is set.

    @param stream The stream to read from. If the lifetime of `stream` ends
    before the coroutine finishes, the behavior is undefined.

    @param buffers The buffer sequence to fill. If the lifetime of the buffer
    sequence represented by `buffers` ends before the coroutine finishes, the behavior is undefined.

    @return An awaitable yielding `results<std::size_t>` whose second
    element is the number of bytes read.

    @par Remarks
    Supports _ContextAwaitable cancellation_.

    A stream may have a member `read( buffers )` that fills the
    whole sequence in one operation. That operation then replaces
    the loop and spares a coroutine frame. Its result must follow
    the rules above. Otherwise the awaitable is a `task`.


    @par Example

    @par !example example


    @see @ref ReadStream, @ref MutableBufferSequence
*/
template <typename S, typename MB>
  requires ReadStream<S> && MutableBufferSequence<MB>
auto
read(S& stream, MB buffers)
{
    if constexpr (detail::stream_reads_all<S, MB>)
        return stream.read(buffers);
    else
        return detail::read_loop(stream, std::move(buffers));
}

} // namespace capy
} // namespace boost

#endif

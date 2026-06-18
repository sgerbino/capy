//
// Copyright (c) 2026 Steve Gerbino
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//
// Official repository: https://github.com/cppalliance/capy
//

#ifndef BOOST_CAPY_FILE_SINK_HPP
#define BOOST_CAPY_FILE_SINK_HPP

#include <boost/capy/detail/config.hpp>
#include <boost/capy/buffers.hpp>
#include <boost/capy/ex/io_env.hpp>
#include <boost/capy/file.hpp>
#include <boost/capy/io_result.hpp>

#include <coroutine>
#include <cstddef>
#include <span>
#include <system_error>
#include <vector>

namespace boost {
namespace capy {

/** A @ref BufferSink adapter over a @ref file.

    Wraps a file with a callee-owned staging buffer so a file can terminate
    zero-copy transfer pipelines. @ref prepare hands out writable staging
    space; @ref commit writes the bytes the caller filled in to the file.

    @par Thread Safety
    Distinct objects: Safe.
    Shared objects: Unsafe.

    @see file, BufferSink
*/
class file_sink
{
    file* f_;
    std::vector<unsigned char> buf_;

public:
    /** Construct a sink over a file.

        @param f The file to write to. Must outlive the sink.
        @param chunk The staging buffer size in bytes.
    */
    explicit file_sink(file& f, std::size_t chunk = 65536)
        : f_(&f)
        , buf_(chunk)
    {
    }

    /** Provide writable staging space.

        @param dest The span of buffer descriptors to fill.

        @return A span of one mutable buffer into staging storage, or empty
        if `dest` is empty.
    */
    std::span<mutable_buffer>
    prepare(std::span<mutable_buffer> dest)
    {
        if(dest.empty())
            return {};
        dest[0] = mutable_buffer(buf_.data(), buf_.size());
        return dest.first(1);
    }

    /** Commit bytes written to the staging buffer to the file.

        @param n The number of bytes written to the last @ref prepare buffer.
        Values exceeding the staging buffer size are clamped.

        @return An awaitable that await-returns `(error_code)`.
    */
    auto
    commit(std::size_t n)
    {
        struct awaitable
        {
            file_sink* self_;
            std::size_t n_;

            bool await_ready() const noexcept { return true; }

            void await_suspend(std::coroutine_handle<>, io_env const*) const noexcept { } // LCOV_EXCL_LINE await_ready is always true, so this never runs

            io_result<>
            await_resume()
            {
                // Never write past the staging buffer the caller was given.
                std::size_t const n = n_ > self_->buf_.size()
                    ? self_->buf_.size() : n_;
                std::error_code ec;
                self_->f_->write(self_->buf_.data(), n, ec);
                return {ec};
            }
        };
        return awaitable{this, n};
    }

    /** Commit final bytes and signal end-of-stream.

        A file has no stream-termination marker, so this commits and
        finalizes as a no-op.

        @param n The number of bytes written to the last @ref prepare buffer.

        @return An awaitable that await-returns `(error_code)`.
    */
    auto
    commit_eof(std::size_t n)
    {
        return commit(n);
    }
};

} // namespace capy
} // namespace boost

#endif

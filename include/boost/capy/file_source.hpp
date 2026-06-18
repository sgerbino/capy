//
// Copyright (c) 2026 Steve Gerbino
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//
// Official repository: https://github.com/cppalliance/capy
//

#ifndef BOOST_CAPY_FILE_SOURCE_HPP
#define BOOST_CAPY_FILE_SOURCE_HPP

#include <boost/capy/detail/config.hpp>
#include <boost/capy/buffers.hpp>
#include <boost/capy/error.hpp>
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

/** A @ref BufferSource adapter over a @ref file.

    Wraps a file with a callee-owned staging buffer so a file can feed
    zero-copy transfer pipelines. Each @ref pull reads a chunk into the
    staging buffer and hands out a `const_buffer` describing the unconsumed
    portion; @ref consume advances past processed bytes.

    @par Thread Safety
    Distinct objects: Safe.
    Shared objects: Unsafe.

    @see file, BufferSource
*/
class file_source
{
    file* f_;
    std::vector<unsigned char> buf_;
    std::size_t filled_ = 0;
    std::size_t consumed_ = 0;

public:
    /** Construct a source over a file.

        @param f The file to read from. Must outlive the source.
        @param chunk The staging buffer size in bytes.
    */
    explicit file_source(file& f, std::size_t chunk = 65536)
        : f_(&f)
        , buf_(chunk)
    {
    }

    /** Pull buffer data from the file.

        Returns unconsumed staged data if any remains; otherwise refills the
        staging buffer with one read. At end-of-stream await-returns
        `(cond::eof, {})`.

        @param dest The span of buffer descriptors to fill.

        @return An awaitable that await-returns
        `(error_code, std::span<const_buffer>)`.
    */
    auto
    pull(std::span<const_buffer> dest)
    {
        struct awaitable
        {
            file_source* self_;
            std::span<const_buffer> dest_;

            bool await_ready() const noexcept { return true; }

            void await_suspend(std::coroutine_handle<>, io_env const*) const noexcept { } // LCOV_EXCL_LINE await_ready is always true, so this never runs

            io_result<std::span<const_buffer>>
            await_resume()
            {
                if(dest_.empty())
                    return {{}, {}};

                if(self_->consumed_ >= self_->filled_)
                {
                    self_->consumed_ = 0;
                    self_->filled_ = 0;
                    std::error_code ec;
                    std::size_t n = self_->f_->read(
                        self_->buf_.data(), self_->buf_.size(), ec);
                    if(ec)
                        return {ec, {}};
                    if(n == 0)
                        return {error::eof, {}};
                    self_->filled_ = n;
                }

                dest_[0] = const_buffer(
                    self_->buf_.data() + self_->consumed_,
                    self_->filled_ - self_->consumed_);
                return {{}, dest_.first(1)};
            }
        };
        return awaitable{this, dest};
    }

    /** Advance past consumed bytes.

        @param n The number of bytes consumed from the last @ref pull.
    */
    void
    consume(std::size_t n) noexcept
    {
        consumed_ += n;
        if(consumed_ > filled_)
            consumed_ = filled_;
    }
};

} // namespace capy
} // namespace boost

#endif

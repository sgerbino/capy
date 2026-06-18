//
// Copyright (c) 2026 Steve Gerbino
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//
// Official repository: https://github.com/cppalliance/capy
//

#ifndef BOOST_CAPY_FILE_HPP
#define BOOST_CAPY_FILE_HPP

#include <boost/capy/detail/config.hpp>
#include <boost/capy/detail/except.hpp>
#include <boost/capy/buffers.hpp>
#include <boost/capy/error.hpp>
#include <boost/capy/ex/io_env.hpp>
#include <boost/capy/io_result.hpp>

#include <coroutine>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <system_error>

namespace boost {
namespace capy {

/** File open modes.

    These select the access, sharing, seeking, and creation behavior of
    @ref file when a path is opened.

    @see file
*/
enum class file_mode
{
    /// Random read-only access to an existing file.
    read,

    /// Sequential read-only access to an existing file.
    scan,

    /// Random read/write access; create the file or truncate it to zero.
    write,

    /// Random read/write access; the file must not already exist.
    write_new,

    /// Random read/write access; the file must already exist.
    write_existing,

    /// Write-only sequential append; create the file if absent.
    append,

    /// Write-only sequential append; the file must already exist.
    append_existing
};

/** A portable synchronous file handle satisfying Capy's stream concepts.

    `file` wraps a platform file (POSIX descriptor, Win32 `HANDLE`, or a
    stdio `FILE*`) behind a uniform synchronous interface. Beyond the
    blocking handle surface (`open`/`read`/`write`/`seek`/...), it provides
    concept-conforming members that return immediately-ready awaitables, so
    generic algorithms written against @ref ReadStream, @ref ReadSource,
    @ref WriteStream, and @ref WriteSink work against a file without an async
    runtime. The callee-owned-storage concepts @ref BufferSource and
    @ref BufferSink are provided by the `file_source` and `file_sink`
    adapters.

    The awaitables never suspend (their `await_ready()` returns `true`), so
    the I/O completes synchronously inside `await_resume()`. Consequently
    cancellation via the I/O environment's stop token has no effect on a
    file operation.

    @par Thread Safety
    Distinct objects: Safe.
    Shared objects: Unsafe.

    @par Example
    @code
    file f( "data.txt", file_mode::read );
    char buf[ 1024 ];
    std::error_code ec;
    std::size_t n = f.read( buf, sizeof( buf ), ec );
    @endcode

    @see file_mode, file_source, file_sink, ReadSource, WriteSink
*/
class file
{
public:
#if defined(BOOST_CAPY_FILE_WIN32)
    /// The native handle type (a Win32 `HANDLE`).
    using native_handle_type = void*;
#elif defined(BOOST_CAPY_FILE_POSIX)
    /// The native handle type (a POSIX file descriptor).
    using native_handle_type = int;
#else
    /// The native handle type (a stdio stream).
    using native_handle_type = std::FILE*;
#endif

private:
    native_handle_type h_ = invalid_handle();

    /// Return the sentinel value representing a closed file.
    static native_handle_type
    invalid_handle() noexcept
    {
#if defined(BOOST_CAPY_FILE_WIN32)
        // INVALID_HANDLE_VALUE without pulling in <windows.h>.
        return reinterpret_cast<void*>(~static_cast<std::uintptr_t>(0));
#elif defined(BOOST_CAPY_FILE_POSIX)
        return -1;
#else
        return nullptr;
#endif
    }

public:
    /// Destroy the file, closing it if open.
    ~file()
    {
        std::error_code ec;
        close(ec);
    }

    /// Construct a closed file.
    file() = default;

    /** Construct and open a file.

        @par Exception Safety
        Strong guarantee.

        @param path The path to open.
        @param mode The open mode.

        @throws std::system_error If the file cannot be opened.
    */
    file(char const* path, file_mode mode)
    {
        open(path, mode);
    }

    /// Construct by transferring ownership; `other` becomes closed.
    file(file&& other) noexcept
        : h_(other.h_)
    {
        other.h_ = invalid_handle();
    }

    /// Assign by transferring ownership; closes any current file first.
    file&
    operator=(file&& other) noexcept
    {
        if(this != &other)
        {
            std::error_code ec;
            close(ec);
            h_ = other.h_;
            other.h_ = invalid_handle();
        }
        return *this;
    }

    file(file const&) = delete;
    file& operator=(file const&) = delete;

    /// Return `true` if the file is open.
    bool
    is_open() const noexcept
    {
        return h_ != invalid_handle();
    }

    /// Return the native handle, or the invalid sentinel if closed.
    native_handle_type
    native_handle() const noexcept
    {
        return h_;
    }

    /** Adopt a native handle, closing any currently open file.

        @param h The handle to take ownership of.
    */
    void
    native_handle(native_handle_type h)
    {
        std::error_code ec;
        close(ec);
        h_ = h;
    }

    /** Close the file.

        @throws std::system_error If closing fails.
    */
    void
    close()
    {
        std::error_code ec;
        close(ec);
        if(ec)
            detail::throw_system_error(ec);
    }

    /// Close the file, reporting failure via `ec`. Never throws.
    BOOST_CAPY_DECL void
    close(std::error_code& ec) noexcept;

    /** Open a file.

        Any currently open file is closed first.

        @throws std::system_error If the file cannot be opened.
    */
    void
    open(char const* path, file_mode mode)
    {
        std::error_code ec;
        open(path, mode, ec);
        if(ec)
            detail::throw_system_error(ec);
    }

    /// Open a file, reporting failure via `ec`. Never throws.
    BOOST_CAPY_DECL void
    open(char const* path, file_mode mode, std::error_code& ec) noexcept;

    /** Return the size of the file in bytes.

        @throws std::system_error On failure.
    */
    std::uint64_t
    size() const
    {
        std::error_code ec;
        auto n = size(ec);
        if(ec)
            detail::throw_system_error(ec);
        return n;
    }

    /// Return the size of the file in bytes, reporting failure via `ec`.
    BOOST_CAPY_DECL std::uint64_t
    size(std::error_code& ec) const noexcept;

    /** Return the current file position.

        @throws std::system_error On failure.
    */
    std::uint64_t
    pos() const
    {
        std::error_code ec;
        auto n = pos(ec);
        if(ec)
            detail::throw_system_error(ec);
        return n;
    }

    /// Return the current file position, reporting failure via `ec`.
    BOOST_CAPY_DECL std::uint64_t
    pos(std::error_code& ec) const noexcept;

    /** Set the current file position to an absolute offset.

        @throws std::system_error On failure.
    */
    void
    seek(std::uint64_t offset)
    {
        std::error_code ec;
        seek(offset, ec);
        if(ec)
            detail::throw_system_error(ec);
    }

    /// Set the current file position, reporting failure via `ec`.
    BOOST_CAPY_DECL void
    seek(std::uint64_t offset, std::error_code& ec) noexcept;

    /** Read raw bytes from the file at the current position.

        Reads up to `n` bytes. A return of `0` indicates end-of-file and is
        not itself an error. The position advances by the number of bytes
        read.

        @throws std::system_error On failure.

        @return The number of bytes read.
    */
    std::size_t
    read(void* buf, std::size_t n)
    {
        std::error_code ec;
        auto r = read(buf, n, ec);
        if(ec)
            detail::throw_system_error(ec);
        return r;
    }

    /// Read raw bytes, reporting failure via `ec`. `0` means end-of-file.
    BOOST_CAPY_DECL std::size_t
    read(void* buf, std::size_t n, std::error_code& ec) noexcept;

    /** Write raw bytes to the file at the current position.

        Writes all `n` bytes unless an error occurs. The position advances
        by the number of bytes written.

        @throws std::system_error On failure.

        @return The number of bytes written.
    */
    std::size_t
    write(void const* buf, std::size_t n)
    {
        std::error_code ec;
        auto r = write(buf, n, ec);
        if(ec)
            detail::throw_system_error(ec);
        return r;
    }

    /// Write raw bytes, reporting failure via `ec`.
    BOOST_CAPY_DECL std::size_t
    write(void const* buf, std::size_t n, std::error_code& ec) noexcept;

    //
    // Concept-conforming members (ready awaitables).
    //
    // Each *_some operation acts on the first non-empty buffer of the
    // sequence (one syscall); the complete operations span the whole
    // sequence. This keeps behavior identical across all backends.
    //

    /** Asynchronously read some bytes into a buffer sequence.

        Satisfies @ref ReadStream. At end-of-stream await-returns
        `(cond::eof, 0)`.

        @param buffers The mutable buffer sequence to fill.

        @return An awaitable that await-returns `(error_code, std::size_t)`.
    */
    template<MutableBufferSequence MB>
    auto
    read_some(MB buffers)
    {
        struct awaitable
        {
            file* self_;
            MB buffers_;

            bool await_ready() const noexcept { return true; }

            void await_suspend(std::coroutine_handle<>, io_env const*) const noexcept { } // LCOV_EXCL_LINE await_ready is always true, so this never runs

            io_result<std::size_t>
            await_resume()
            {
                for(auto it = capy::begin(buffers_);
                    it != capy::end(buffers_); ++it)
                {
                    mutable_buffer b(*it);
                    if(b.size() == 0)
                        continue;
                    std::error_code ec;
                    std::size_t n = self_->read(b.data(), b.size(), ec);
                    if(ec)
                        return {ec, 0};
                    if(n == 0)
                        return {error::eof, 0};
                    return {{}, n};
                }
                return {{}, 0};
            }
        };
        return awaitable{this, buffers};
    }

    /** Asynchronously read into and fill a buffer sequence.

        Satisfies @ref ReadSource. Fills the entire sequence; a partial fill
        at end-of-stream await-returns `(cond::eof, n)`.

        @param buffers The mutable buffer sequence to fill.

        @return An awaitable that await-returns `(error_code, std::size_t)`.
    */
    template<MutableBufferSequence MB>
    auto
    read(MB buffers)
    {
        struct awaitable
        {
            file* self_;
            MB buffers_;

            bool await_ready() const noexcept { return true; }

            void await_suspend(std::coroutine_handle<>, io_env const*) const noexcept { } // LCOV_EXCL_LINE await_ready is always true, so this never runs

            io_result<std::size_t>
            await_resume()
            {
                std::size_t total = 0;
                for(auto it = capy::begin(buffers_);
                    it != capy::end(buffers_); ++it)
                {
                    mutable_buffer b(*it);
                    std::size_t off = 0;
                    while(off < b.size())
                    {
                        std::error_code ec;
                        std::size_t n = self_->read(
                            static_cast<char*>(b.data()) + off,
                            b.size() - off, ec);
                        if(ec)
                            return {ec, total};
                        if(n == 0)
                            return {error::eof, total};
                        off += n;
                        total += n;
                    }
                }
                return {{}, total};
            }
        };
        return awaitable{this, buffers};
    }

    /** Asynchronously write some bytes from a buffer sequence.

        Satisfies @ref WriteStream.

        @param buffers The const buffer sequence to write.

        @return An awaitable that await-returns `(error_code, std::size_t)`.
    */
    template<ConstBufferSequence CB>
    auto
    write_some(CB buffers)
    {
        struct awaitable
        {
            file* self_;
            CB buffers_;

            bool await_ready() const noexcept { return true; }

            void await_suspend(std::coroutine_handle<>, io_env const*) const noexcept { } // LCOV_EXCL_LINE await_ready is always true, so this never runs

            io_result<std::size_t>
            await_resume()
            {
                for(auto it = capy::begin(buffers_);
                    it != capy::end(buffers_); ++it)
                {
                    const_buffer b(*it);
                    if(b.size() == 0)
                        continue;
                    std::error_code ec;
                    std::size_t n = self_->write(b.data(), b.size(), ec);
                    if(ec)
                        return {ec, n};
                    return {{}, n};
                }
                return {{}, 0};
            }
        };
        return awaitable{this, buffers};
    }

    /** Asynchronously write an entire buffer sequence.

        Satisfies the complete-write requirement of @ref WriteSink.

        @param buffers The const buffer sequence to write.

        @return An awaitable that await-returns `(error_code, std::size_t)`.
    */
    template<ConstBufferSequence CB>
    auto
    write(CB buffers)
    {
        struct awaitable
        {
            file* self_;
            CB buffers_;

            bool await_ready() const noexcept { return true; }

            void await_suspend(std::coroutine_handle<>, io_env const*) const noexcept { } // LCOV_EXCL_LINE await_ready is always true, so this never runs

            io_result<std::size_t>
            await_resume()
            {
                std::size_t total = 0;
                for(auto it = capy::begin(buffers_);
                    it != capy::end(buffers_); ++it)
                {
                    const_buffer b(*it);
                    if(b.size() == 0)
                        continue;
                    std::error_code ec;
                    std::size_t n = self_->write(b.data(), b.size(), ec);
                    total += n;
                    if(ec)
                        return {ec, total};
                }
                return {{}, total};
            }
        };
        return awaitable{this, buffers};
    }

    /** Asynchronously write an entire buffer sequence and signal EOF.

        Satisfies @ref WriteSink. A file has no stream-termination marker,
        so this writes the buffers and finalizes as a no-op.

        @param buffers The const buffer sequence to write.

        @return An awaitable that await-returns `(error_code, std::size_t)`.
    */
    template<ConstBufferSequence CB>
    auto
    write_eof(CB buffers)
    {
        return write(buffers);
    }

    /** Asynchronously signal EOF with no data.

        Satisfies @ref WriteSink. A no-op for a file.

        @return An awaitable that await-returns `(error_code)`.
    */
    auto
    write_eof()
    {
        struct awaitable
        {
            bool await_ready() const noexcept { return true; }

            void await_suspend(std::coroutine_handle<>, io_env const*) const noexcept { } // LCOV_EXCL_LINE await_ready is always true, so this never runs

            io_result<>
            await_resume() noexcept
            {
                return {};
            }
        };
        return awaitable{};
    }
};

} // namespace capy
} // namespace boost

#endif

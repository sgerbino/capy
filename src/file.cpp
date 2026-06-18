//
// Copyright (c) 2026 Steve Gerbino
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//
// Official repository: https://github.com/cppalliance/capy
//

#include <boost/capy/file.hpp>

#if defined(BOOST_CAPY_FILE_POSIX)
# include <cerrno>
# include <fcntl.h>
# include <sys/stat.h>
# include <unistd.h>
#elif defined(BOOST_CAPY_FILE_WIN32)
# ifndef NOMINMAX
#  define NOMINMAX
# endif
# ifndef WIN32_LEAN_AND_MEAN
#  define WIN32_LEAN_AND_MEAN
# endif
# include <windows.h>
#else
# include <cerrno>
# include <cstdio>
#endif

namespace boost {
namespace capy {

//----------------------------------------------------------
// POSIX backend
//----------------------------------------------------------

#if defined(BOOST_CAPY_FILE_POSIX)

void
file::
close(std::error_code& ec) noexcept
{
    ec.clear();
    if(h_ == invalid_handle())
        return;
    int const fd = h_;
    h_ = invalid_handle();
    if(::close(fd) != 0)
        ec.assign(errno, std::generic_category());
}

void
file::
open(char const* path, file_mode mode, std::error_code& ec) noexcept
{
    {
        std::error_code ignored;
        close(ignored);
    }
    ec.clear();

    int flags = 0;
    switch(mode)
    {
    case file_mode::read:
    case file_mode::scan:
        flags = O_RDONLY;
        break;
    case file_mode::write:
        flags = O_RDWR | O_CREAT | O_TRUNC;
        break;
    case file_mode::write_new:
        flags = O_RDWR | O_CREAT | O_EXCL;
        break;
    case file_mode::write_existing:
        flags = O_RDWR;
        break;
    case file_mode::append:
        flags = O_WRONLY | O_CREAT | O_APPEND;
        break;
    case file_mode::append_existing:
        flags = O_WRONLY | O_APPEND;
        break;
    }

    int fd;
    do
    {
        fd = ::open(path, flags, 0644);
    }
    while(fd < 0 && errno == EINTR);

    if(fd < 0)
    {
        ec.assign(errno, std::generic_category());
        return;
    }
    h_ = fd;
}

std::uint64_t
file::
size(std::error_code& ec) const noexcept
{
    ec.clear();
    struct ::stat st;
    if(::fstat(h_, &st) != 0)
    {
        ec.assign(errno, std::generic_category());
        return 0;
    }
    return static_cast<std::uint64_t>(st.st_size);
}

std::uint64_t
file::
pos(std::error_code& ec) const noexcept
{
    ec.clear();
    ::off_t const r = ::lseek(h_, 0, SEEK_CUR);
    if(r < 0)
    {
        ec.assign(errno, std::generic_category());
        return 0;
    }
    return static_cast<std::uint64_t>(r);
}

void
file::
seek(std::uint64_t offset, std::error_code& ec) noexcept
{
    ec.clear();
    if(::lseek(h_, static_cast<::off_t>(offset), SEEK_SET) < 0)
        ec.assign(errno, std::generic_category());
}

std::size_t
file::
read(void* buf, std::size_t n, std::error_code& ec) noexcept
{
    ec.clear();
    if(n == 0)
        return 0;
    ::ssize_t r;
    do
    {
        r = ::read(h_, buf, n);
    }
    while(r < 0 && errno == EINTR);
    if(r < 0)
    {
        ec.assign(errno, std::generic_category());
        return 0;
    }
    return static_cast<std::size_t>(r);
}

std::size_t
file::
write(void const* buf, std::size_t n, std::error_code& ec) noexcept
{
    ec.clear();
    std::size_t total = 0;
    auto p = static_cast<char const*>(buf);
    while(total < n)
    {
        ::ssize_t r;
        do
        {
            r = ::write(h_, p + total, n - total);
        }
        while(r < 0 && errno == EINTR);
        if(r < 0)
        {
            ec.assign(errno, std::generic_category());
            return total;
        }
        if(r == 0)
            break; // LCOV_EXCL_LINE ::write never returns 0 for a positive count on a regular file
        total += static_cast<std::size_t>(r);
    }
    return total;
}

//----------------------------------------------------------
// Win32 backend
//----------------------------------------------------------

#elif defined(BOOST_CAPY_FILE_WIN32)

namespace {

std::error_code
last_error() noexcept
{
    return std::error_code(
        static_cast<int>(::GetLastError()), std::system_category());
}

} // namespace

void
file::
close(std::error_code& ec) noexcept
{
    ec.clear();
    if(h_ == invalid_handle())
        return;
    HANDLE const h = h_;
    h_ = invalid_handle();
    if(! ::CloseHandle(h))
        ec = last_error();
}

void
file::
open(char const* path, file_mode mode, std::error_code& ec) noexcept
{
    {
        std::error_code ignored;
        close(ignored);
    }
    ec.clear();

    DWORD access = 0;
    DWORD share = 0;
    DWORD creation = 0;
    DWORD flags = FILE_ATTRIBUTE_NORMAL;

    switch(mode)
    {
    case file_mode::read:
        access = GENERIC_READ;
        share = FILE_SHARE_READ;
        creation = OPEN_EXISTING;
        break;
    case file_mode::scan:
        access = GENERIC_READ;
        share = FILE_SHARE_READ;
        creation = OPEN_EXISTING;
        flags = FILE_FLAG_SEQUENTIAL_SCAN;
        break;
    case file_mode::write:
        access = GENERIC_READ | GENERIC_WRITE;
        creation = CREATE_ALWAYS;
        break;
    case file_mode::write_new:
        access = GENERIC_READ | GENERIC_WRITE;
        creation = CREATE_NEW;
        break;
    case file_mode::write_existing:
        access = GENERIC_READ | GENERIC_WRITE;
        creation = OPEN_EXISTING;
        break;
    case file_mode::append:
        access = FILE_APPEND_DATA;
        creation = OPEN_ALWAYS;
        break;
    case file_mode::append_existing:
        access = FILE_APPEND_DATA;
        creation = OPEN_EXISTING;
        break;
    }

    HANDLE h = ::CreateFileA(
        path, access, share, nullptr, creation, flags, nullptr);
    if(h == INVALID_HANDLE_VALUE)
    {
        ec = last_error();
        return;
    }
    h_ = h;
}

std::uint64_t
file::
size(std::error_code& ec) const noexcept
{
    ec.clear();
    LARGE_INTEGER li;
    if(! ::GetFileSizeEx(h_, &li))
    {
        ec = last_error();
        return 0;
    }
    return static_cast<std::uint64_t>(li.QuadPart);
}

std::uint64_t
file::
pos(std::error_code& ec) const noexcept
{
    ec.clear();
    LARGE_INTEGER zero;
    zero.QuadPart = 0;
    LARGE_INTEGER out;
    if(! ::SetFilePointerEx(h_, zero, &out, FILE_CURRENT))
    {
        ec = last_error();
        return 0;
    }
    return static_cast<std::uint64_t>(out.QuadPart);
}

void
file::
seek(std::uint64_t offset, std::error_code& ec) noexcept
{
    ec.clear();
    LARGE_INTEGER li;
    li.QuadPart = static_cast<LONGLONG>(offset);
    if(! ::SetFilePointerEx(h_, li, nullptr, FILE_BEGIN))
        ec = last_error();
}

std::size_t
file::
read(void* buf, std::size_t n, std::error_code& ec) noexcept
{
    ec.clear();
    if(n == 0)
        return 0;
    DWORD const want = n > 0xffffffffu
        ? 0xffffffffu : static_cast<DWORD>(n);
    DWORD got = 0;
    if(! ::ReadFile(h_, buf, want, &got, nullptr))
    {
        ec = last_error();
        return 0;
    }
    return static_cast<std::size_t>(got);
}

std::size_t
file::
write(void const* buf, std::size_t n, std::error_code& ec) noexcept
{
    ec.clear();
    std::size_t total = 0;
    auto p = static_cast<char const*>(buf);
    while(total < n)
    {
        std::size_t const left = n - total;
        DWORD const want = left > 0xffffffffu
            ? 0xffffffffu : static_cast<DWORD>(left);
        DWORD put = 0;
        if(! ::WriteFile(h_, p + total, want, &put, nullptr))
        {
            ec = last_error();
            return total;
        }
        if(put == 0)
            break;
        total += static_cast<std::size_t>(put);
    }
    return total;
}

//----------------------------------------------------------
// stdio fallback backend
//----------------------------------------------------------

#else

void
file::
close(std::error_code& ec) noexcept
{
    ec.clear();
    if(h_ == invalid_handle())
        return;
    std::FILE* const fp = h_;
    h_ = invalid_handle();
    if(std::fclose(fp) != 0)
        ec.assign(errno, std::generic_category());
}

void
file::
open(char const* path, file_mode mode, std::error_code& ec) noexcept
{
    {
        std::error_code ignored;
        close(ignored);
    }
    ec.clear();

    char const* m = nullptr;
    switch(mode)
    {
    case file_mode::read:
    case file_mode::scan:
        m = "rb";
        break;
    case file_mode::write:
        m = "wb+";
        break;
    case file_mode::write_new:
        // Read/write to match the other backends; "x" is C11 exclusive.
        m = "wb+x";
        break;
    case file_mode::write_existing:
        m = "rb+";
        break;
    case file_mode::append:
        m = "ab";
        break;
    case file_mode::append_existing:
        // No standard mode combines must-exist with append, so require the
        // file to exist first, then open "ab" for true end-of-file writes.
        {
            std::FILE* probe = std::fopen(path, "rb");
            if(! probe)
            {
                ec.assign(errno, std::generic_category());
                return;
            }
            std::fclose(probe);
        }
        m = "ab";
        break;
    }

    std::FILE* fp = std::fopen(path, m);
    if(! fp)
    {
        ec.assign(errno, std::generic_category());
        return;
    }
    h_ = fp;
}

std::uint64_t
file::
size(std::error_code& ec) const noexcept
{
    // long-based ftell/fseek cap offsets at 2 GiB where long is 32-bit;
    // this fallback only runs on platforms that are neither POSIX nor Win32.
    ec.clear();
    long const cur = std::ftell(h_);
    if(cur < 0 ||
        std::fseek(h_, 0, SEEK_END) != 0)
    {
        ec.assign(errno, std::generic_category());
        return 0;
    }
    long const end = std::ftell(h_);
    if(end < 0 ||
        std::fseek(h_, cur, SEEK_SET) != 0)
    {
        ec.assign(errno, std::generic_category());
        return 0;
    }
    return static_cast<std::uint64_t>(end);
}

std::uint64_t
file::
pos(std::error_code& ec) const noexcept
{
    ec.clear();
    long const r = std::ftell(h_);
    if(r < 0)
    {
        ec.assign(errno, std::generic_category());
        return 0;
    }
    return static_cast<std::uint64_t>(r);
}

void
file::
seek(std::uint64_t offset, std::error_code& ec) noexcept
{
    ec.clear();
    if(std::fseek(h_, static_cast<long>(offset), SEEK_SET) != 0)
        ec.assign(errno, std::generic_category());
}

std::size_t
file::
read(void* buf, std::size_t n, std::error_code& ec) noexcept
{
    ec.clear();
    if(n == 0)
        return 0;
    std::size_t const r = std::fread(buf, 1, n, h_);
    if(r < n && std::ferror(h_))
    {
        ec.assign(errno, std::generic_category());
        return 0;
    }
    return r;
}

std::size_t
file::
write(void const* buf, std::size_t n, std::error_code& ec) noexcept
{
    ec.clear();
    std::size_t const r = std::fwrite(buf, 1, n, h_);
    if(r < n)
        ec.assign(errno, std::generic_category());
    return r;
}

#endif

} // namespace capy
} // namespace boost

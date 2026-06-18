//
// Copyright (c) 2026 Steve Gerbino
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//
// Official repository: https://github.com/cppalliance/capy
//

// Test that header file is self-contained.
#include <boost/capy/file.hpp>

#include <boost/capy/file_sink.hpp>
#include <boost/capy/file_source.hpp>
#include <boost/capy/buffers/buffer_copy.hpp>
#include <boost/capy/buffers/make_buffer.hpp>
#include <boost/capy/concept/buffer_sink.hpp>
#include <boost/capy/concept/buffer_source.hpp>
#include <boost/capy/concept/read_source.hpp>
#include <boost/capy/concept/read_stream.hpp>
#include <boost/capy/concept/write_sink.hpp>
#include <boost/capy/concept/write_stream.hpp>
#include <boost/capy/cond.hpp>
#include <boost/capy/error.hpp>
#include <boost/capy/read.hpp>
#include <boost/capy/task.hpp>
#include <boost/capy/test/run_blocking.hpp>
#include <boost/capy/write.hpp>

#include "test_suite.hpp"

#include <array>
#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <span>
#include <string>
#include <string_view>
#include <system_error>

namespace boost {
namespace capy {

//----------------------------------------------------------
// Concept conformance
//----------------------------------------------------------

static_assert(ReadStream<file>);
static_assert(ReadSource<file>);
static_assert(WriteStream<file>);
static_assert(WriteSink<file>);
static_assert(BufferSource<file_source>);
static_assert(BufferSink<file_sink>);

namespace {

// A self-deleting unique path under the temp directory.
class temp_path
{
    std::filesystem::path p_;

public:
    explicit temp_path(char const* tag)
    {
        static int counter = 0;
        // The static's address differs per process (ASLR), giving a
        // portable process token so concurrent runs don't collide.
        auto token = reinterpret_cast<std::uintptr_t>(&counter);
        p_ = std::filesystem::temp_directory_path() /
            (std::string("capy_file_test_") + tag + "_" +
                std::to_string(token) + "_" + std::to_string(++counter));
        std::filesystem::remove(p_);
    }

    ~temp_path()
    {
        std::error_code ec;
        std::filesystem::remove(p_, ec);
    }

    temp_path(temp_path const&) = delete;
    temp_path& operator=(temp_path const&) = delete;

    char const* c_str() const noexcept { return p_.c_str(); }
    bool exists() const { return std::filesystem::exists(p_); }
};

} // namespace

struct file_test
{
    //----------------------------------------------------------
    // Synchronous handle surface
    //----------------------------------------------------------

    void
    testDefaultConstruct()
    {
        file f;
        BOOST_TEST(! f.is_open());
    }

    void
    testOpenClose()
    {
        temp_path tp("openclose");
        file f;
        std::error_code ec;
        f.open(tp.c_str(), file_mode::write, ec);
        BOOST_TEST(! ec);
        BOOST_TEST(f.is_open());
        f.close(ec);
        BOOST_TEST(! ec);
        BOOST_TEST(! f.is_open());
    }

    void
    testConstructOpens()
    {
        temp_path tp("ctoropen");
        file f(tp.c_str(), file_mode::write);
        BOOST_TEST(f.is_open());
    }

    void
    testWriteReadRoundTrip()
    {
        temp_path tp("roundtrip");
        std::string_view msg = "hello world";

        {
            file f(tp.c_str(), file_mode::write);
            std::error_code ec;
            std::size_t n = f.write(msg.data(), msg.size(), ec);
            BOOST_TEST(! ec);
            BOOST_TEST_EQ(n, msg.size());
        }

        {
            file f(tp.c_str(), file_mode::read);
            BOOST_TEST_EQ(f.size(), msg.size());
            char buf[32];
            std::error_code ec;
            std::size_t n = f.read(buf, sizeof(buf), ec);
            BOOST_TEST(! ec);
            BOOST_TEST_EQ(n, msg.size());
            BOOST_TEST_EQ(std::string_view(buf, n), msg);
        }
    }

    void
    testReadEof()
    {
        temp_path tp("eof");
        {
            file f(tp.c_str(), file_mode::write);
            f.write("abc", 3);
        }
        file f(tp.c_str(), file_mode::read);
        char buf[8];
        std::error_code ec;
        std::size_t n = f.read(buf, sizeof(buf), ec);
        BOOST_TEST(! ec);
        BOOST_TEST_EQ(n, 3u);
        // Subsequent read at EOF returns 0 with no error (handle semantics).
        n = f.read(buf, sizeof(buf), ec);
        BOOST_TEST(! ec);
        BOOST_TEST_EQ(n, 0u);
    }

    void
    testSeekPos()
    {
        temp_path tp("seek");
        file f(tp.c_str(), file_mode::write);
        f.write("0123456789", 10);

        std::error_code ec;
        f.seek(4, ec);
        BOOST_TEST(! ec);
        BOOST_TEST_EQ(f.pos(), 4u);

        char buf[4];
        std::size_t n = f.read(buf, 3, ec);
        BOOST_TEST(! ec);
        BOOST_TEST_EQ(n, 3u);
        BOOST_TEST_EQ(std::string_view(buf, 3), "456");
        BOOST_TEST_EQ(f.pos(), 7u);
    }

    void
    testAppend()
    {
        temp_path tp("append");
        {
            file f(tp.c_str(), file_mode::write);
            f.write("aaa", 3);
        }
        {
            file f(tp.c_str(), file_mode::append);
            f.write("bbb", 3);
        }
        file f(tp.c_str(), file_mode::read);
        char buf[16];
        std::error_code ec;
        std::size_t n = f.read(buf, sizeof(buf), ec);
        BOOST_TEST(! ec);
        BOOST_TEST_EQ(std::string_view(buf, n), "aaabbb");
    }

    void
    testAppendExisting()
    {
        temp_path tp("appendexisting");
        {
            file f(tp.c_str(), file_mode::write);
            f.write("aaa", 3);
        }
        {
            file f(tp.c_str(), file_mode::append_existing);
            f.write("bbb", 3);
        }
        {
            file f(tp.c_str(), file_mode::read);
            char buf[16];
            std::error_code ec;
            std::size_t n = f.read(buf, sizeof(buf), ec);
            BOOST_TEST(! ec);
            BOOST_TEST_EQ(std::string_view(buf, n), "aaabbb");
        }

        // append_existing must fail when the file does not exist.
        temp_path missing("appendexisting_missing");
        file g;
        std::error_code ec;
        g.open(missing.c_str(), file_mode::append_existing, ec);
        BOOST_TEST(ec.value() != 0);
        BOOST_TEST(! g.is_open());
    }

    //----------------------------------------------------------
    // Error reporting
    //----------------------------------------------------------

    void
    testOpenMissingFails()
    {
        temp_path tp("missing");
        file f;
        std::error_code ec;
        f.open(tp.c_str(), file_mode::read, ec);
        BOOST_TEST(ec.value() != 0);
        BOOST_TEST(! f.is_open());
    }

    void
    testOpenMissingThrows()
    {
        temp_path tp("missingthrows");
        file f;
        BOOST_TEST_THROWS(
            f.open(tp.c_str(), file_mode::read),
            std::system_error);
    }

    void
    testWriteNewExistingFails()
    {
        temp_path tp("writenew");
        {
            file f(tp.c_str(), file_mode::write);
        }
        // File now exists; write_new must fail.
        file f;
        std::error_code ec;
        f.open(tp.c_str(), file_mode::write_new, ec);
        BOOST_TEST(ec.value() != 0);
    }

    void
    testWriteExistingMissingFails()
    {
        temp_path tp("writeexisting");
        file f;
        std::error_code ec;
        f.open(tp.c_str(), file_mode::write_existing, ec);
        BOOST_TEST(ec.value() != 0);
    }

#if defined(BOOST_CAPY_FILE_POSIX)
    void
    testCloseError()
    {
        // Adopt a descriptor that is not open; closing it must report failure.
        file f;
        f.native_handle(99999);
        BOOST_TEST(f.is_open());
        std::error_code ec;
        f.close(ec);
        BOOST_TEST(ec.value() != 0);
        BOOST_TEST(! f.is_open());
    }
#endif

    void
    testClosedOperationsThrow()
    {
        // Every operation on a closed file hits an invalid descriptor and
        // must surface the failure through the throwing overload.
        file f;
        char buf[8];
        BOOST_TEST_THROWS(f.size(), std::system_error);
        BOOST_TEST_THROWS(f.pos(), std::system_error);
        BOOST_TEST_THROWS(f.seek(0), std::system_error);
        BOOST_TEST_THROWS(f.read(buf, sizeof(buf)), std::system_error);
        BOOST_TEST_THROWS(f.write("x", 1), std::system_error);
    }

    void
    testAwaitablesPropagateErrors()
    {
        // A closed file fails each underlying syscall; the concept members
        // must report a real error (not eof) via their error branch.
        test::run_blocking()([&]() -> task<void>
        {
            file f;
            char buf[8];
            auto [ec1, n1] = co_await f.read_some(
                mutable_buffer(buf, sizeof(buf)));
            BOOST_TEST(ec1.value() != 0);
            BOOST_TEST(ec1 != cond::eof);
            BOOST_TEST_EQ(n1, 0u);

            auto [ec2, n2] = co_await f.read(
                mutable_buffer(buf, sizeof(buf)));
            BOOST_TEST(ec2.value() != 0);

            auto [ec3, n3] = co_await f.write_some(
                make_buffer(std::string_view("x")));
            BOOST_TEST(ec3.value() != 0);

            auto [ec4, n4] = co_await f.write(
                make_buffer(std::string_view("x")));
            BOOST_TEST(ec4.value() != 0);
            co_return;
        }());
    }

    void
    testEmptyBuffers()
    {
        // Empty buffers are skipped, not errors: the operation completes
        // with success and zero bytes without touching the file.
        temp_path tp("empty");
        test::run_blocking()([&]() -> task<void>
        {
            file f(tp.c_str(), file_mode::write);
            auto [ec1, n1] = co_await f.read_some(mutable_buffer());
            BOOST_TEST(! ec1);
            BOOST_TEST_EQ(n1, 0u);

            auto [ec2, n2] = co_await f.read(mutable_buffer());
            BOOST_TEST(! ec2);
            BOOST_TEST_EQ(n2, 0u);

            auto [ec3, n3] = co_await f.write_some(const_buffer());
            BOOST_TEST(! ec3);
            BOOST_TEST_EQ(n3, 0u);

            auto [ec4, n4] = co_await f.write(const_buffer());
            BOOST_TEST(! ec4);
            BOOST_TEST_EQ(n4, 0u);
            co_return;
        }());
    }

    //----------------------------------------------------------
    // Native handle and move
    //----------------------------------------------------------

    void
    testNativeHandle()
    {
        temp_path tp("native");
        file f(tp.c_str(), file_mode::write);
        BOOST_TEST(f.native_handle() != file().native_handle());
    }

    void
    testMove()
    {
        temp_path tp("move");
        file f(tp.c_str(), file_mode::write);
        auto h = f.native_handle();
        file g(std::move(f));
        BOOST_TEST(! f.is_open());
        BOOST_TEST(g.is_open());
        BOOST_TEST(g.native_handle() == h);
    }

    //----------------------------------------------------------
    // Concept members (ready awaitables)
    //----------------------------------------------------------

    void
    testReadSomeWriteSome()
    {
        temp_path tp("readsome");
        std::string_view msg = "concept bytes";

        test::run_blocking()([&]() -> task<void>
        {
            file f(tp.c_str(), file_mode::write);
            auto [wec, wn] = co_await f.write_some(make_buffer(msg));
            BOOST_TEST(! wec);
            BOOST_TEST_EQ(wn, msg.size());
            f.seek(0);

            char buf[64];
            auto [rec, rn] = co_await f.read_some(
                mutable_buffer(buf, sizeof(buf)));
            BOOST_TEST(! rec);
            BOOST_TEST_EQ(rn, msg.size());
            BOOST_TEST_EQ(std::string_view(buf, rn), msg);
            co_return;
        }());
    }

    void
    testReadSomeEof()
    {
        temp_path tp("readsomeeof");
        test::run_blocking()([&]() -> task<void>
        {
            {
                file w(tp.c_str(), file_mode::write);
                auto [wec, wn] =
                    co_await w.write_some(make_buffer(std::string_view("xy")));
                BOOST_TEST(! wec);
                BOOST_TEST_EQ(wn, 2u);
            }
            file f(tp.c_str(), file_mode::read);
            char buf[8];
            auto [ec1, n1] = co_await f.read_some(
                mutable_buffer(buf, sizeof(buf)));
            BOOST_TEST(! ec1);
            BOOST_TEST_EQ(n1, 2u);
            auto [ec2, n2] = co_await f.read_some(
                mutable_buffer(buf, sizeof(buf)));
            BOOST_TEST(ec2 == cond::eof);
            BOOST_TEST_EQ(n2, 0u);
            co_return;
        }());
    }

    void
    testCompleteReadSource()
    {
        temp_path tp("readsource");
        std::string_view msg = "exactly-sized";
        test::run_blocking()([&]() -> task<void>
        {
            {
                file w(tp.c_str(), file_mode::write);
                auto [wec, wn] = co_await w.write(make_buffer(msg));
                BOOST_TEST(! wec);
                BOOST_TEST_EQ(wn, msg.size());
            }
            file f(tp.c_str(), file_mode::read);
            char buf[13];
            BOOST_TEST_EQ(sizeof(buf), msg.size());
            auto [ec, n] = co_await f.read(
                mutable_buffer(buf, sizeof(buf)));
            BOOST_TEST(! ec);
            BOOST_TEST_EQ(n, msg.size());
            BOOST_TEST_EQ(std::string_view(buf, n), msg);
            co_return;
        }());
    }

    void
    testCompleteReadSourceEof()
    {
        temp_path tp("readsourceeof");
        test::run_blocking()([&]() -> task<void>
        {
            {
                file w(tp.c_str(), file_mode::write);
                auto [wec, wn] =
                    co_await w.write(make_buffer(std::string_view("ab")));
                BOOST_TEST(! wec);
                BOOST_TEST_EQ(wn, 2u);
            }
            file f(tp.c_str(), file_mode::read);
            char buf[8];  // larger than file
            auto [ec, n] = co_await f.read(
                mutable_buffer(buf, sizeof(buf)));
            BOOST_TEST(ec == cond::eof);
            BOOST_TEST_EQ(n, 2u);
            co_return;
        }());
    }

    void
    testWriteEof()
    {
        temp_path tp("writeeof");
        std::string_view msg = "with-eof";
        test::run_blocking()([&]() -> task<void>
        {
            file f(tp.c_str(), file_mode::write);
            auto [ec, n] = co_await f.write_eof(make_buffer(msg));
            BOOST_TEST(! ec);
            BOOST_TEST_EQ(n, msg.size());
            co_return;
        }());

        file f(tp.c_str(), file_mode::read);
        char buf[32];
        std::error_code ec;
        std::size_t n = f.read(buf, sizeof(buf), ec);
        BOOST_TEST_EQ(std::string_view(buf, n), msg);
    }

    void
    testWriteEofEmpty()
    {
        temp_path tp("writeeofempty");
        test::run_blocking()([&]() -> task<void>
        {
            file f(tp.c_str(), file_mode::write);
            auto [ec] = co_await f.write_eof();
            BOOST_TEST(! ec);
            co_return;
        }());
    }

    void
    testCompleteMultiBuffer()
    {
        // Exercises the per-buffer accumulation loop in write(CB)/read(MB),
        // which single-buffer tests never reach.
        temp_path tp("multibuf");
        test::run_blocking()([&]() -> task<void>
        {
            {
                file w(tp.c_str(), file_mode::write);
                std::array<const_buffer, 2> wb{{
                    make_buffer(std::string_view("hello")),
                    make_buffer(std::string_view("world"))
                }};
                auto [wec, wn] = co_await w.write(wb);
                BOOST_TEST(! wec);
                BOOST_TEST_EQ(wn, 10u);
            }
            file r(tp.c_str(), file_mode::read);
            char a[5];
            char b[5];
            std::array<mutable_buffer, 2> rb{{
                mutable_buffer(a, sizeof(a)),
                mutable_buffer(b, sizeof(b))
            }};
            auto [rec, rn] = co_await r.read(rb);
            BOOST_TEST(! rec);
            BOOST_TEST_EQ(rn, 10u);
            BOOST_TEST_EQ(std::string_view(a, 5), "hello");
            BOOST_TEST_EQ(std::string_view(b, 5), "world");
            co_return;
        }());
    }

    //----------------------------------------------------------
    // Generic algorithm integration
    //----------------------------------------------------------

    void
    testAlgorithmRoundTrip()
    {
        temp_path tp("algo");
        std::string_view msg = "algorithm round trip payload";
        test::run_blocking()([&]() -> task<void>
        {
            {
                file w(tp.c_str(), file_mode::write);
                auto [wec, wn] = co_await capy::write(w, make_buffer(msg));
                BOOST_TEST(! wec);
                BOOST_TEST_EQ(wn, msg.size());
            }
            file r(tp.c_str(), file_mode::read);
            std::string out(msg.size(), '\0');
            auto [rec, rn] = co_await capy::read(
                r, mutable_buffer(out.data(), out.size()));
            BOOST_TEST(! rec);
            BOOST_TEST_EQ(rn, msg.size());
            BOOST_TEST_EQ(out, msg);
            co_return;
        }());
    }

    //----------------------------------------------------------
    // Buffer source / sink adapters
    //----------------------------------------------------------

    void
    testBufferTransfer()
    {
        temp_path src("xfer_src");
        temp_path dst("xfer_dst");
        std::string msg(200000, 'Z');  // spans multiple 64KiB chunks

        {
            file w(src.c_str(), file_mode::write);
            w.write(msg.data(), msg.size());
        }

        test::run_blocking()([&]() -> task<void>
        {
            file in(src.c_str(), file_mode::read);
            file out(dst.c_str(), file_mode::write);
            file_source source(in);
            file_sink sink(out);

            const_buffer src_arr[4];
            mutable_buffer dst_arr[4];
            for(;;)
            {
                auto [pec, bufs] = co_await source.pull(src_arr);
                if(pec == cond::eof)
                {
                    auto [eec] = co_await sink.commit_eof(0);
                    BOOST_TEST(! eec);
                    break;
                }
                BOOST_TEST(! pec);
                auto out_bufs = sink.prepare(dst_arr);
                std::size_t n = buffer_copy(out_bufs, bufs);
                auto [cec] = co_await sink.commit(n);
                BOOST_TEST(! cec);
                source.consume(n);
            }
            co_return;
        }());

        file check(dst.c_str(), file_mode::read);
        BOOST_TEST_EQ(check.size(), msg.size());
        std::string out(msg.size(), '\0');
        std::error_code ec;
        std::size_t n = check.read(out.data(), out.size(), ec);
        BOOST_TEST(! ec);
        BOOST_TEST_EQ(n, msg.size());
        BOOST_TEST(out == msg);
    }

    void
    testBufferPartialConsume()
    {
        // A sink smaller than the source chunk forces each pull to be
        // consumed in pieces, exercising the unconsumed-remainder path.
        temp_path src("partial_src");
        temp_path dst("partial_dst");
        std::string msg = "the quick brown fox jumps over the lazy dog";

        {
            file w(src.c_str(), file_mode::write);
            w.write(msg.data(), msg.size());
        }

        test::run_blocking()([&]() -> task<void>
        {
            file in(src.c_str(), file_mode::read);
            file out(dst.c_str(), file_mode::write);
            file_source source(in, 64);
            file_sink sink(out, 7);

            const_buffer src_arr[1];
            mutable_buffer dst_arr[1];
            for(;;)
            {
                auto [pec, bufs] = co_await source.pull(src_arr);
                if(pec == cond::eof)
                {
                    auto [eec] = co_await sink.commit_eof(0);
                    BOOST_TEST(! eec);
                    break;
                }
                BOOST_TEST(! pec);
                auto out_bufs = sink.prepare(dst_arr);
                std::size_t n = buffer_copy(out_bufs, bufs);
                BOOST_TEST(n <= 7u);
                auto [cec] = co_await sink.commit(n);
                BOOST_TEST(! cec);
                source.consume(n);
            }
            co_return;
        }());

        file check(dst.c_str(), file_mode::read);
        std::string out(msg.size(), '\0');
        std::error_code ec;
        std::size_t n = check.read(out.data(), out.size(), ec);
        BOOST_TEST(! ec);
        BOOST_TEST_EQ(n, msg.size());
        BOOST_TEST_EQ(out, msg);
    }

    void
    testSourceSinkEdges()
    {
        temp_path tp("edges");
        {
            file w(tp.c_str(), file_mode::write);
            w.write("abcdef", 6);
        }

        test::run_blocking()([&]() -> task<void>
        {
            // pull from a closed file surfaces a real error (not eof).
            file closed;
            file_source bad(closed);
            const_buffer arr[2];
            auto [ec, bufs] = co_await bad.pull(arr);
            BOOST_TEST(ec.value() != 0);
            BOOST_TEST(ec != cond::eof);

            // An empty destination span yields success with no buffers.
            auto [ec2, bufs2] =
                co_await bad.pull(std::span<const_buffer>{});
            BOOST_TEST(! ec2);
            BOOST_TEST(bufs2.empty());

            // Over-consuming clamps to the filled amount; the next pull
            // then refills and reaches end-of-file.
            file in(tp.c_str(), file_mode::read);
            file_source source(in, 64);
            auto [ec3, bufs3] = co_await source.pull(arr);
            BOOST_TEST(! ec3);
            source.consume(1000000);
            auto [ec4, bufs4] = co_await source.pull(arr);
            BOOST_TEST(ec4 == cond::eof);
            co_return;
        }());

        // An empty prepare span yields an empty result.
        file out(tp.c_str(), file_mode::write);
        file_sink sink(out);
        auto s = sink.prepare(std::span<mutable_buffer>{});
        BOOST_TEST(s.empty());
    }

    void
    testRawZeroAndThrowingRead()
    {
        temp_path tp("rawzero");
        file f(tp.c_str(), file_mode::write);
        f.write("ab", 2);
        f.seek(0);

        char buf[4];
        std::error_code ec;
        // A zero-length raw read is a no-op success.
        std::size_t z = f.read(buf, 0, ec);
        BOOST_TEST(! ec);
        BOOST_TEST_EQ(z, 0u);

        // Throwing read overload on a successful read.
        std::size_t r = f.read(buf, 2);
        BOOST_TEST_EQ(r, 2u);
        BOOST_TEST_EQ(std::string_view(buf, 2), "ab");
    }

    void
    run()
    {
        testDefaultConstruct();
        testOpenClose();
        testConstructOpens();
        testWriteReadRoundTrip();
        testReadEof();
        testSeekPos();
        testAppend();
        testAppendExisting();
        testOpenMissingFails();
        testOpenMissingThrows();
        testWriteNewExistingFails();
        testWriteExistingMissingFails();
#if defined(BOOST_CAPY_FILE_POSIX)
        testCloseError();
#endif
        testClosedOperationsThrow();
        testAwaitablesPropagateErrors();
        testEmptyBuffers();
        testNativeHandle();
        testMove();
        testReadSomeWriteSome();
        testReadSomeEof();
        testCompleteReadSource();
        testCompleteReadSourceEof();
        testWriteEof();
        testWriteEofEmpty();
        testCompleteMultiBuffer();
        testAlgorithmRoundTrip();
        testBufferTransfer();
        testBufferPartialConsume();
        testSourceSinkEdges();
        testRawZeroAndThrowingRead();
    }
};

TEST_SUITE(
    file_test,
    "boost.capy.file");

} // namespace capy
} // namespace boost

//
// Copyright (c) 2025 Vinnie Falco (vinnie.falco@gmail.com)
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//
// Official repository: https://github.com/cppalliance/capy
//

#include <boost/capy/test/run_blocking.hpp>

#include <boost/capy/ex/chain_owner.hpp>
#include <boost/capy/ex/frame_allocator.hpp>
#include <boost/capy/ex/ready_queue.hpp>

#include <condition_variable>
#include <mutex>
#include <system_error>
#include <utility>

namespace boost {
namespace capy {
namespace test {

namespace {

// The context whose run() is on this thread's stack. Only there is it
// safe to resume dispatched work inline.
thread_local blocking_context const* running_ = nullptr;

} // namespace

struct blocking_context::impl
{
    std::mutex mtx;
    std::condition_variable cv;
    ready_queue<> queue;
    std::exception_ptr ep;
    bool done = false;
    bool abandoned = false;
};

blocking_context::blocking_context()
    : impl_(new impl)
{
}

blocking_context::~blocking_context()
{
    // Nothing will run these now. Each is taken under the lock and
    // dropped outside it, since abandoning one may post another.
    auto drain = [this]
    {
        for(;;)
        {
            coroutine_chain_holder h;
            {
                std::lock_guard<std::mutex> lock(impl_->mtx);
                h = impl_->queue.pop();
            }
            if(!h)
                return;
        }
    };
    shutdown();
    // Takes what the unwinds post too, and the work of any service they
    // create, which is shut down as it is created.
    drain();
    destroy();
    // Services give up their work in shutdown(), but anything a
    // destructor posts anyway is still destroyed.
    drain();
    delete impl_;
}

blocking_executor
blocking_context::get_executor() noexcept
{
    return blocking_executor{this};
}

void
blocking_context::signal_done() noexcept
{
    std::lock_guard<std::mutex> lock(impl_->mtx);
    impl_->done = true;
    impl_->cv.notify_one();
}

void
blocking_context::signal_done(
    std::exception_ptr ep) noexcept
{
    std::lock_guard<std::mutex> lock(impl_->mtx);
    impl_->ep = ep;
    impl_->done = true;
    impl_->cv.notify_one();
}

void
blocking_context::signal_abandoned() noexcept
{
    std::lock_guard<std::mutex> lock(impl_->mtx);
    impl_->abandoned = true;
    impl_->done = true;
    impl_->cv.notify_one();
}

void
blocking_context::run()
{
    struct scoped_running
    {
        blocking_context const* prev_;

        explicit scoped_running(blocking_context const* ctx) noexcept
            : prev_(std::exchange(running_, ctx))
        {
        }

        ~scoped_running()
        {
            running_ = prev_;
        }
    } guard(this);

    for(;;)
    {
        coroutine_chain_holder h;
        {
            std::unique_lock<std::mutex> lock(impl_->mtx);
            impl_->cv.wait(lock, [&] {
                return impl_->done || !impl_->queue.empty();
            });
            if(impl_->done && impl_->queue.empty())
                break;
            h = impl_->queue.pop();
        }
        h.resume();
    }
    if(impl_->ep)
        std::rethrow_exception(impl_->ep);
    if(impl_->abandoned)
        throw std::system_error(error::canceled, "task abandoned");
}

void
blocking_context::enqueue(
    coroutine_chain_holder&& h)
{
    std::lock_guard<std::mutex> lock(impl_->mtx);
    impl_->queue.push(std::move(h));
    impl_->cv.notify_one();
}

//----------------------------------------------------------

bool
blocking_executor::operator==(
    blocking_executor const& other) const noexcept
{
    return ctx_ == other.ctx_;
}

blocking_context&
blocking_executor::context() const noexcept
{
    return *ctx_;
}

void
blocking_executor::on_work_started() const noexcept
{
}

void
blocking_executor::on_work_finished() const noexcept
{
}

std::coroutine_handle<>
blocking_executor::dispatch(
    coroutine_chain_holder&& h) const
{
    if(running_ == ctx_)
        return h.take_handle();
    ctx_->enqueue(std::move(h));
    return std::noop_coroutine();
}

void
blocking_executor::post(
    coroutine_chain_holder&& h) const
{
    ctx_->enqueue(std::move(h));
}

} // namespace test
} // namespace capy
} // namespace boost

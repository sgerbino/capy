//
// Copyright (c) 2025 Vinnie Falco (vinnie.falco@gmail.com)
// Copyright (c) 2026 Michael Vandeberg
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//
// Official repository: https://github.com/boostorg/capy
//

#include <boost/capy/ex/thread_pool.hpp>
#include <boost/capy/test/thread_name.hpp>
#include <algorithm>
#include <cstdio>

namespace boost {
namespace capy {

thread_pool::
~thread_pool()
{
    stop();
    join();
    shutdown();
    destroy();
}

thread_pool::
thread_pool(std::size_t num_threads, std::string_view thread_name_prefix)
    : num_threads_(num_threads)
{
    if(num_threads_ == 0)
        num_threads_ = std::max(
            std::thread::hardware_concurrency(), 1u);

    auto n = thread_name_prefix.copy(thread_name_prefix_, 12);
    thread_name_prefix_[n] = '\0';

    this->set_frame_allocator(std::allocator<void>{});
}

void
thread_pool::
join() noexcept
{
    {
        std::unique_lock<std::mutex> lock(mutex_);
        if(joined_)
            return;
        joined_ = true;

        if(outstanding_work_.load(
            std::memory_order_acquire) == 0)
        {
            stop_ = true;
            cv_.notify_all();
        }
        else
        {
            cv_.wait(lock, [this]{
                return stop_;
            });
        }
    }

    for(auto& t : threads_)
        if(t.joinable())
            t.join();
}

void
thread_pool::
stop() noexcept
{
    {
        std::lock_guard<std::mutex> lock(mutex_);
        stop_ = true;
    }
    cv_.notify_all();
}

void
thread_pool::
run(std::size_t index)
{
    char name[16];
    std::snprintf(name, sizeof(name), "%s%zu", thread_name_prefix_, index);
    set_current_thread_name(name);

    for(;;)
    {
        work_item* w = nullptr;
        {
            std::unique_lock<std::mutex> lock(mutex_);
            cv_.wait(lock, [this]{
                return !q_.empty() || stop_;
            });
            if(stop_)
                return;
            w = q_.pop();
        }
        if(w)
            w->execute();
    }
}

} // capy
} // boost

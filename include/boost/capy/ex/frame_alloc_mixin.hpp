//
// Copyright (c) 2026 Michael Vandeberg
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//
// Official repository: https://github.com/cppalliance/capy
//

#ifndef BOOST_CAPY_EX_FRAME_ALLOC_MIXIN_HPP
#define BOOST_CAPY_EX_FRAME_ALLOC_MIXIN_HPP

#include <boost/capy/detail/config.hpp>
#include <boost/capy/ex/frame_allocator.hpp>
#include <boost/capy/ex/recycling_memory_resource.hpp>

#include <coroutine>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory_resource>
#include <utility>

namespace boost {
namespace capy {
namespace detail {

// A frame's memory may come from a resource stored in an ancestor frame,
// so a parent destroyed from a child's promise destructor is destroyed
// only after the child's frame is deallocated. The recorder's address
// keys the deferral to its own frame: frames owned by its parameters
// are freed in between and must not drain it.
struct destroy_after_free_state
{
    std::coroutine_handle<> pending;
    void const* recorder = nullptr;
    bool draining = false;
};

#if BOOST_CAPY_WORKAROUND(_MSC_VER, >= 1) && !defined(__clang__)
__declspec(noinline)
#endif
inline destroy_after_free_state&
destroy_after_free_tls() noexcept
{
    static thread_local destroy_after_free_state s;
    return s;
}

/** Record a coroutine to destroy once the current frame is freed.

    Called from a promise destructor, so the parent it records is
    destroyed after the frame being destroyed has been deallocated.

    @par Preconditions
    No coroutine is already recorded. @p recorder lies within the
    frame being destroyed, and that frame is deallocated by
    `frame_alloc_mixin::operator delete` or by an `operator delete`
    that calls @ref drain_destroy_after_free after deallocating.

    @param h The coroutine to destroy.

    @param recorder The address of the promise being destroyed.
*/
inline void
destroy_after_free(
    std::coroutine_handle<> h, void const* recorder) noexcept
{
    auto& s = destroy_after_free_tls();
    BOOST_CAPY_ASSERT(!s.pending);
    s.pending = h;
    s.recorder = recorder;
}

/** Destroy recorded coroutines once their recording frame is freed.

    Does nothing unless the pending coroutine was recorded from within
    the frame that occupied `[frame, frame + size)`. A call made while
    another call on the same thread is draining returns immediately,
    and the outer call destroys whatever the frames it destroys
    record, so a whole chain is destroyed iteratively instead of
    recursively.

    @param frame The address of the frame just deallocated.

    @param size The size of that frame, as passed to `operator delete`.
*/
inline void
drain_destroy_after_free(void const* frame, std::size_t size) noexcept
{
    auto& s = destroy_after_free_tls();
    if(!s.pending || s.draining)
        return;
    // Only the integer values are compared; the frame is already freed.
    auto const lo = reinterpret_cast<std::uintptr_t>(frame);
    auto const at = reinterpret_cast<std::uintptr_t>(s.recorder);
    if(at - lo >= size)
        return;
    s.draining = true;
    while(auto h = std::exchange(s.pending, nullptr))
        h.destroy();
    s.draining = false;
}

} // namespace detail

/** Mixin that adds frame-allocator-aware allocation to a promise type.

    Inherit from this class in any coroutine promise type to opt into
    TLS-based frame allocation with the recycling memory resource
    fast path. The mixin provides `operator new` and `operator delete`
    that:

    1. Read the thread-local frame allocator set by `run_async` or `run`.
    2. Bypass virtual dispatch when the allocator is the default
       recycling memory resource.
    3. Store the allocator pointer at the end of each frame for
       correct deallocation even when TLS changes between allocation
       and deallocation.

    This is the same allocation strategy used by @ref
    io_awaitable_promise_base. Use this mixin directly when your
    promise type does not need the full environment and continuation
    support that `io_awaitable_promise_base` provides.

    @par Example
    @par !example example


    @par Thread Safety
    The allocation fast path uses thread-local storage and requires
    no synchronization. The global pool fallback is mutex-protected.

    @see io_awaitable_promise_base, frame_allocator, recycling_memory_resource
*/
struct frame_alloc_mixin
{
    /** Allocate a coroutine frame.

        Uses the thread-local frame allocator set by run_async.
        Falls back to default memory resource if not set.
        Stores the allocator pointer at the end of each frame for
        correct deallocation even when TLS changes. Uses memcpy
        to avoid alignment requirements on the trailing pointer.
        Bypasses virtual dispatch for the recycling allocator.

        @param size The size, in bytes, of the coroutine frame.

        @return A pointer to storage for the frame.

        @par Exception Safety
        Propagates any exception thrown by the underlying memory
        resource's `allocate`, for example `std::bad_alloc` from
        `::operator new`.
    */
    static void* operator new(std::size_t size)
    {
        static auto* const rmr = get_recycling_memory_resource();

        auto* mr = get_current_frame_allocator();
        if(!mr)
            mr = std::pmr::get_default_resource();

        auto total = size + sizeof(std::pmr::memory_resource*);
        void* raw;
        if(mr == rmr)
            raw = static_cast<recycling_memory_resource*>(mr)
                ->allocate_fast(total, alignof(std::max_align_t));
        else
            raw = mr->allocate(total, alignof(std::max_align_t));
        std::memcpy(static_cast<char*>(raw) + size, &mr, sizeof(mr));
        return raw;
    }

    /** Deallocate a coroutine frame.

        Reads the allocator pointer stored at the end of the frame
        to ensure correct deallocation regardless of current TLS.
        Bypasses virtual dispatch for the recycling allocator.
        Then destroys any parent recorded by
        `detail::destroy_after_free` while this frame was destroyed.

        @param ptr The frame storage returned by `operator new`.

        @param size The size, in bytes, that was passed to `operator new`.
        The allocator pointer is read from `ptr + size`, which is where
        `operator new` wrote it, so this value must match.
    */
    static void operator delete(void* ptr, std::size_t size) noexcept
    {
        static auto* const rmr = get_recycling_memory_resource();

        std::pmr::memory_resource* mr;
        std::memcpy(&mr, static_cast<char*>(ptr) + size, sizeof(mr));
        auto total = size + sizeof(std::pmr::memory_resource*);
        if(mr == rmr)
            static_cast<recycling_memory_resource*>(mr)
                ->deallocate_fast(ptr, total, alignof(std::max_align_t));
        else
            mr->deallocate(ptr, total, alignof(std::max_align_t));
        detail::drain_destroy_after_free(ptr, size);
    }
};

} // namespace capy
} // namespace boost

#endif

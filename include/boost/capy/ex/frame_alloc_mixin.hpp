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
#include <exception>
#include <memory_resource>
#include <utility>

namespace boost {
namespace capy {
namespace detail {

// A frame's memory may come from a resource stored in an ancestor frame,
// so a parent destroyed from a child's promise destructor is destroyed
// only after the child's frame is deallocated. Each deferral is keyed to
// its recorder's address: a frame's parameters may destroy other frames
// between its promise destructor and its deallocation, and those record
// their own parents while the outer deferral is still pending.
struct destroy_after_free_entry
{
    std::coroutine_handle<> h;
    void const* recorder = nullptr;
    bool ready = false;
};

// Fixed capacity because teardown must not allocate. Pending entries
// are bounded by the frames simultaneously between their promise
// destructor and their deallocation on one thread. Ready entries pile
// up only under an outer drain, and are destroyed early when the
// table fills.
struct destroy_after_free_state
{
    static constexpr std::size_t capacity = 32;
    destroy_after_free_entry entries[capacity];
    std::size_t size = 0;
    bool draining = false;
};

#if BOOST_CAPY_WORKAROUND(_MSC_VER, >= 1) && !defined(__clang__)
__declspec(noinline)
#endif
inline destroy_after_free_state&
destroy_after_free_tls() noexcept
{
    static thread_local constinit destroy_after_free_state s;
    return s;
}

/** Record a coroutine to destroy once the current frame is freed.

    Called from a promise destructor, so the parent it records is
    destroyed after the frame being destroyed has been deallocated.

    Several coroutines may be recorded at once on one thread: a frame
    whose parameters destroy other frames before it is deallocated has
    its own deferral pending while those frames record theirs. Each
    recorded coroutine is destroyed only after the frame containing
    its @p recorder has been deallocated.

    @par Preconditions
    @p recorder lies within the frame being destroyed, and that frame
    is deallocated by `frame_alloc_mixin::operator delete` or by an
    `operator delete` that calls @ref drain_destroy_after_free after
    deallocating.

    @note Calls `std::terminate` if 32 deferrals whose frames are
    not yet deallocated are already outstanding on the calling
    thread, since teardown cannot allocate.

    @param h The coroutine to destroy.

    @param recorder The address of the promise being destroyed.
*/
inline void
destroy_after_free(
    std::coroutine_handle<> h, void const* recorder) noexcept
{
    auto& s = destroy_after_free_tls();
    while(s.size == s.capacity)
    {
        // Only an outer drain leaves entries ready; their frames are
        // already freed, so destroying them now is what it would do.
        std::size_t i = 0;
        while(i < s.size && !s.entries[i].ready)
            ++i;
        if(i == s.size)
            std::terminate();
        auto ready = s.entries[i].h;
        s.entries[i] = s.entries[--s.size];
        ready.destroy();
    }
    s.entries[s.size++] = {h, recorder, false};
}

/** Destroy recorded coroutines once their recording frame is freed.

    Marks ready every coroutine recorded from within the frame that
    occupied `[frame, frame + size)`, then destroys ready coroutines
    until none remain. Coroutines recorded from frames not yet
    deallocated stay recorded. A call made while another call on the
    same thread is draining only marks, and the outer call destroys
    whatever becomes ready while it runs, so a whole chain is
    destroyed iteratively instead of recursively.

    @note A heap-elided frame is never deallocated, so a coroutine
    type whose frame can record must not be elided into its parent.

    @param frame The address of the frame just deallocated.

    @param size The size of that frame, as passed to `operator delete`.
*/
inline void
drain_destroy_after_free(void const* frame, std::size_t size) noexcept
{
    auto& s = destroy_after_free_tls();
    if(s.size == 0)
        return;
    // Only the integer values are compared; the frame is already freed.
    auto const lo = reinterpret_cast<std::uintptr_t>(frame);
    for(std::size_t i = 0; i < s.size; ++i)
    {
        auto const at =
            reinterpret_cast<std::uintptr_t>(s.entries[i].recorder);
        if(at - lo < size)
            s.entries[i].ready = true;
    }
    if(s.draining)
        return;
    s.draining = true;
    for(;;)
    {
        std::size_t i = 0;
        while(i < s.size && !s.entries[i].ready)
            ++i;
        if(i == s.size)
            break;
        auto h = s.entries[i].h;
        s.entries[i] = s.entries[--s.size];
        h.destroy();
    }
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
        `detail::destroy_after_free` while this frame was destroyed,
        or leaves it to a drain already running on this thread.

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

//
// Copyright (c) 2026 Steve Gerbino
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//
// Official repository: https://github.com/cppalliance/capy
//

#ifndef BOOST_CAPY_EX_READY_QUEUE_HPP
#define BOOST_CAPY_EX_READY_QUEUE_HPP

#include <boost/capy/detail/config.hpp>
#include <boost/capy/continuation.hpp>
#include <boost/capy/ex/coroutine_chain_holder.hpp>

#include <cstdint>
#include <type_traits>
#include <utility>

namespace boost {
namespace capy {

/** Base class giving an operation a link for @ref ready_queue.

    Derive an operation type from this to queue it alongside coroutines
    with no allocation per entry.
*/
class ready_node
{
    void* ready_next_ = nullptr;

    template<class>
    friend class ready_queue;
};

/** An intrusive FIFO of coroutines ready to resume, with no allocation.

    Executors need a queue of suspended coroutines, and each coroutine
    must be given up if the queue dies first. This queue links entries
    through a slot in the @ref continuation that only this type can
    touch, and owns each entry the way a @ref coroutine_chain_holder does.

    `ready_queue<>` holds only coroutines. `ready_queue<Op>` also holds
    pointers to `Op`, in one order, for a scheduler that mixes
    coroutines with its own operations. It tags each link, so use it
    only when the mix is needed.

    The destructor gives up every queued coroutine. It leaves queued
    `Op` objects alone, as the queue does not own them.

    @par Thread Safety
    Distinct objects: Safe.
    Shared objects: Unsafe.

    @par Example
    @code
    ready_queue<> doomed;
    {
        std::lock_guard lock(mutex_);
        doomed.splice(queue_);
    }
    // The destructor gives them up outside the lock
    @endcode

    @tparam Op The operation type, derived from @ref ready_node, or
        `void` for a queue of coroutines alone.

    @see coroutine_chain_holder
*/
template<class Op = void>
class ready_queue;

template<>
class ready_queue<void>
{
    continuation* head_ = nullptr;
    continuation* tail_ = nullptr;

public:
    /// Destroy the queue, giving up each queued coroutine.
    ~ready_queue()
    {
        clear();
    }

    /// Construct an empty queue.
    ready_queue() = default;

    /** Construct a queue by taking every entry of another.

        @param other The queue to take from; it is empty afterward.
    */
    ready_queue(ready_queue&& other) noexcept
        : head_(std::exchange(other.head_, nullptr))
        , tail_(std::exchange(other.tail_, nullptr))
    {
    }

    ready_queue(ready_queue const&) = delete;
    ready_queue& operator=(ready_queue const&) = delete;
    ready_queue& operator=(ready_queue&&) = delete;

    /// Check whether the queue holds no entries.
    bool empty() const noexcept
    {
        return head_ == nullptr;
    }

    /** Append a coroutine, taking ownership of it.

        @par Preconditions
        `h` owns a coroutine.

        @param h The holder to take from; it owns nothing afterward.
    */
    void push(coroutine_chain_holder&& h) noexcept
    {
        continuation* c = h.release();
        c->reserved_ = nullptr;
        if(tail_)
            tail_->reserved_ = c;
        else
            head_ = c;
        tail_ = c;
    }

    /** Remove the front coroutine.

        @return A holder of the coroutine, or an empty holder if the
            queue is empty.
    */
    coroutine_chain_holder pop() noexcept
    {
        if(!head_)
            return {};
        continuation* c = head_;
        head_ = static_cast<continuation*>(c->reserved_);
        if(!head_)
            tail_ = nullptr;
        return coroutine_chain_holder(*c);
    }

    /** Remove every entry, giving up each queued coroutine.

        Call this without holding a lock an unwind may need.
    */
    void clear() noexcept
    {
        while(head_)
            pop();
    }

    /** Move every entry of another queue to the back of this one.

        @par Complexity
        Constant.

        @param other The queue to take from; it is empty afterward.
    */
    void splice(ready_queue& other) noexcept
    {
        if(!other.head_)
            return;
        if(tail_)
            tail_->reserved_ = other.head_;
        else
            head_ = other.head_;
        tail_ = std::exchange(other.tail_, nullptr);
        other.head_ = nullptr;
    }
};

template<class Op>
class ready_queue
{
    static_assert(std::is_base_of_v<ready_node, Op>);
    static_assert(alignof(Op) >= 2);
    static_assert(alignof(continuation) >= 2);

    // The low bit of a link selects the node kind. Alignment guarantees
    // it is free; a high bit would rest on the address-space layout.
    static constexpr std::uintptr_t cont_bit = 1;

    void* head_ = nullptr;
    void* tail_ = nullptr;

    static bool
    is_cont(void* e) noexcept
    {
        return (reinterpret_cast<std::uintptr_t>(e) & cont_bit) != 0;
    }

    static continuation*
    as_cont(void* e) noexcept
    {
        return reinterpret_cast<continuation*>(
            reinterpret_cast<std::uintptr_t>(e) & ~cont_bit);
    }

    static Op*
    as_op(void* e) noexcept
    {
        return static_cast<Op*>(e);
    }

    // GCC 12/13 diagnose the untaken branch against the smaller object
    // once inlining proves the entry's kind. Fixed in GCC 14.
#if defined(__GNUC__) && !defined(__clang__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Warray-bounds"
#endif
    static void*
    next_of(void* e) noexcept
    {
        if(is_cont(e))
            return as_cont(e)->reserved_;
        return static_cast<ready_node*>(as_op(e))->ready_next_;
    }

    static void
    set_next(void* e, void* next) noexcept
    {
        if(is_cont(e))
            as_cont(e)->reserved_ = next;
        else
            static_cast<ready_node*>(as_op(e))->ready_next_ = next;
    }
#if defined(__GNUC__) && !defined(__clang__)
#pragma GCC diagnostic pop
#endif

    void
    push_entry(void* e) noexcept
    {
        set_next(e, nullptr);
        if(tail_)
            set_next(tail_, e);
        else
            head_ = e;
        tail_ = e;
    }

public:
    /** One entry removed from a @ref ready_queue.

        Owns a coroutine entry until @ref take is called, and gives
        it up otherwise. Refers to an `Op` entry without owning it.
    */
    class entry
    {
        void* e_ = nullptr;

        friend class ready_queue;

        explicit entry(void* e) noexcept
            : e_(e)
        {
        }

    public:
        /// Destroy the entry, giving up a coroutine not yet taken.
        ~entry()
        {
            if(is_continuation())
                take();
        }

        /// Construct an empty entry.
        entry() = default;

        /** Construct an entry by taking from another.

            @param other The entry to take from; it is empty afterward.
        */
        entry(entry&& other) noexcept
            : e_(std::exchange(other.e_, nullptr))
        {
        }

        entry(entry const&) = delete;
        entry& operator=(entry const&) = delete;
        entry& operator=(entry&&) = delete;

        /// Check whether the entry refers to anything.
        explicit operator bool() const noexcept
        {
            return e_ != nullptr;
        }

        /// Check whether the entry owns a coroutine.
        bool is_continuation() const noexcept
        {
            return is_cont(e_);
        }

        /// Return the operation, or null for any other entry.
        Op* op() const noexcept
        {
            return is_cont(e_) ? nullptr : as_op(e_);
        }

        /** Take the coroutine out of the entry.

            @par Preconditions
            `is_continuation()` is true.

            @return A holder of the coroutine.
        */
        coroutine_chain_holder take() noexcept
        {
            return coroutine_chain_holder(*as_cont(std::exchange(e_, nullptr)));
        }
    };

    /// Destroy the queue, giving up each queued coroutine.
    ~ready_queue()
    {
        clear();
    }

    /// Construct an empty queue.
    ready_queue() = default;

    /** Construct a queue by taking every entry of another.

        @param other The queue to take from; it is empty afterward.
    */
    ready_queue(ready_queue&& other) noexcept
        : head_(std::exchange(other.head_, nullptr))
        , tail_(std::exchange(other.tail_, nullptr))
    {
    }

    ready_queue(ready_queue const&) = delete;
    ready_queue& operator=(ready_queue const&) = delete;
    ready_queue& operator=(ready_queue&&) = delete;

    /// Check whether the queue holds no entries.
    bool empty() const noexcept
    {
        return head_ == nullptr;
    }

    /** Append an operation.

        @param op The operation, which must outlive its time queued.
    */
    void push(Op* op) noexcept
    {
        push_entry(op);
    }

    /** Append a coroutine, taking ownership of it.

        @par Preconditions
        `h` owns a coroutine.

        @param h The holder to take from; it owns nothing afterward.
    */
    void push(coroutine_chain_holder&& h) noexcept
    {
        push_entry(reinterpret_cast<void*>(
            reinterpret_cast<std::uintptr_t>(h.release()) | cont_bit));
    }

    /** Remove the front entry.

        @return The entry, which is empty if the queue is empty.
    */
    entry pop() noexcept
    {
        void* e = head_;
        if(!e)
            return entry();
        head_ = next_of(e);
        if(!head_)
            tail_ = nullptr;
        return entry(e);
    }

    /** Remove every entry, giving up each queued coroutine.

        Call this without holding a lock an unwind may need.
    */
    void clear() noexcept
    {
        while(head_)
            pop();
    }

    /** Move every entry of another queue to the back of this one.

        @par Complexity
        Constant.

        @param other The queue to take from; it is empty afterward.
    */
    void splice(ready_queue& other) noexcept
    {
        if(!other.head_)
            return;
        if(tail_)
            set_next(tail_, other.head_);
        else
            head_ = other.head_;
        tail_ = std::exchange(other.tail_, nullptr);
        other.head_ = nullptr;
    }
};

} // namespace capy
} // namespace boost

#endif

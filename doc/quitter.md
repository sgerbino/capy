# `quitter<T>` - Stop-Aware Task Type

## Summary

`quitter<T>` is a task type that satisfies `ContextRunnable`. A stop
request ends it without resuming it. When the stop token fires, the
quitter's frames are destroyed from the top down, as an abandoned
chain's are (`capy::abandon`). Only destructors run. Nothing in the
body observes the stop, and nothing can intercept it, not even
`catch(...)`. The stop is reported, as a value, to the first coroutine
above that is not a quitter. The coroutine author writes no
cancellation-handling code.

The header's docstrings are the reference. This note explains the
design.

## Motivation

Ville Voutilainen (the emeritus chair of EWG in WG21) identified a gap in Capy's `task<T>`:
when an application needs to shut down (e.g. Qt "close all windows"),
every in-flight coroutine should exit cleanly without the programmer
writing `if (ec) co_return;` after every `co_await`. The sender model
achieves this via `set_stopped`: the coroutine never resumes. Capy's
`task<T>` resumes with an error equal to `cond::canceled` and requires
explicit handling.

`quitter<T>` closes this gap. It is a policy-level task type: same
ContextAwaitable protocol, same await_context propagation, same frame
allocator, same symmetric transfer. But a stop request ends it instead
of resuming it.

## Behavior

### Normal operation (stop not requested)

As `task<T>`. The coroutine runs, awaits operations, and returns a
value or throws.

### When stop is requested

1. The stop token in `await_context` is triggered (from a parent, a
   combinator, or application shutdown).
2. The in-flight operation observes the token and completes, typically
   with `cond::canceled`.
3. The operation does not resume the quitter's frame. Every quitter
   frame hands its operations a *gate* as the continuation, so the
   operation resumes the gate.
4. The gate sees the stop and asks the waiting `co_await` whether the
   operation stopped (see Completions). If it did, the gate abandons
   the quitter's *segment*. Its owner destroys the quitter frames, on
   the executor, from the top down. No quitter frame is resumed.
5. The coroutine that awaited the quitter is resumed. Its `co_await`
   yields the stopped value.

If the operation produced a real result instead, because it completed
before it saw the stop, the body receives that result, and the stop
takes effect at the next `co_await`.

A stop that is already requested when the quitter is awaited does not
start the body; the `co_await` yields the stopped value at once.
`run_async` and `run(ex)` start a quitter through its gate, so the
stop is checked when the body would start, not when the launcher is
called: a stop that arrives in between does not start the body either.

### What a stopped quitter delivers

| `T` | stopped value |
|-----|---------------|
| `results<Ts...>` | `make_error_code(error::canceled)`, each `Ts` value-initialized |
| `std::error_code` | `make_error_code(error::canceled)` |
| any other `T`, `void` included | none: only a quitter can await it, and stops with it |

Every `Ts` of a `results<Ts...>` must be default-constructible.

A coroutine that is not a quitter, such as a `task`, can await only a
quitter whose `T` is `results<Ts...>` or `std::error_code`. The same
holds for `run(ex)(q)` and the other `run` overloads. Awaiting any
other quitter there is ill-formed. A `quitter<>` or a plain-`T` quitter
can be awaited by a quitter, which shares its segment and stops
together with it, or started with `run_async`.

`run_async` reports a stopped quitter to its handlers. For `results`
and `error_code`, the value handler gets the stopped value. For `void`
or a plain `T`, the error handler gets a `std::system_error` whose
`code()` compares equal to `cond::canceled`. With no error handler, no
handler is called and the stop is discarded. Such a quitter cannot be
started where exceptions are disabled. Any other exception a task
throws, a `std::system_error` with `canceled` included, still
terminates.

## Segments and the gate

A quitter awaited by anything other than a quitter opens a *segment*
in its `await_suspend`. The segment is one small coroutine frame that
is both the segment's owner (a `chain_owner`, the `await_context::owner`
every frame in the segment sees) and the gate. A quitter awaited by a
quitter joins its parent's segment, so a stop ends both together. A
quitter started directly by `run_async` or `run(ex)` runs in the
launcher's segment and gets a gate of its own at launch. The launcher
resumes that gate to start the body, so the stop is checked first.

A stop abandons only the segment, so it destroys quitter frames only.
A `task` below a quitter receives the stop request as usual and
returns through its own code before the quitter is destroyed. A
`task` above it receives the stopped value.

A gate is resumed by the completion of the one operation a quitter
frame of its segment waits on, or, for the gate through which
`run_async` or `run(ex)` starts a quitter, by the launcher's dispatch.
An owner is resumed by the post that follows an abandon. A gate decides
only once it is resumed, when no operation holds a quitter frame's
continuation, so no frame is destroyed while an operation still holds
its continuation.

## Completions

The stop token is checked before each awaited operation starts, so a
pending stop starts no work.

Once an operation completes with a stop pending, the operation decides
what happens, not the timing of the stop. The same rule holds whether
it completed synchronously (its `await_ready` returns `true`, or its
`await_suspend` returns `false` or the handle it was given) or through
the gate. The operation's `await_resume` is called, because its earlier
steps may already have acquired something, such as `async_mutex`'s
lock, that only `await_resume` hands over. Its result then decides:

| result | the quitter |
|--------|-------------|
| `results<Ts...>` or `std::error_code` holding `cond::canceled` | is stopped at that `co_await`: the operation saw the stop and acquired nothing |
| any other `results<Ts...>` or `std::error_code` | resumes with the result; the stop takes effect at the next `co_await` |
| any other type, `void` included | is stopped at that `co_await`; the result is destroyed |

`async_mutex`, `async_event` and `async_waker` all answer a stop with a
`results<>` holding `cond::canceled`. A result of any other type cannot
say whether the operation stopped, so the stop wins. Delivering it
would let the body go on as if the operation had succeeded. Nothing is
lost by destroying it: whatever it owns is released by its destructor,
and an awaitable that hands over state that is not owned this way
returns a `results`.

An exception that `await_resume` throws is not a stop. It is kept, and
rethrown in the body at the `co_await`, where it would have surfaced
without the stop.

## Interaction with `when_all` / `when_any`

A quitter child opens its own segment, never the combinator's. When
the combinator stops its children, each quitter child is abandoned and
delivers its stopped value to the combinator, which handles it as that
child's result. Both combinators take only `results` children, so a
quitter child delivers `canceled` as a value and throws nothing.

## Restrictions

**Run a quitter through `await_suspend`, not `handle()`.** A custom
awaitable that runs a quitter for an awaiting coroutine calls the
quitter's `await_suspend`, which opens a segment. Resuming
`handle()` with the awaiting coroutine's environment puts the quitter
in that coroutine's segment, so a stop abandons the awaiting
coroutine's own segment, and a `task` chain there is torn down with no
handler called. A debug build asserts when `run_async` or `run` owns
that segment. A launcher that resumes the handle itself must bind a
`chain_owner` of its own.

**An environment with no owner cannot stop a quitter.** A debug build
asserts; otherwise the quitter runs on as a `task` would.

**Structured bindings at a `co_await`.** A stop destroys quitter frames
that are suspended at a `co_await`. GCC 15.2, and GCC 16 before
2026-07-02, at `-O0`, run the destructor of a never-constructed
structured binding when such a frame is destroyed (GCC PR c++/124584).
When an element is not trivially destructible, bind the result to a
named variable and destructure it afterwards:

```cpp
auto r = co_await mtx.scoped_lock();
auto& [ec, guard] = r;
```

**Shutdown.** A stop that arrives while the execution context is being
shut down may end the quitter as a teardown instead, with nothing
delivered. Stop the work, wait for it to finish, and only then destroy
the context.

**Out of memory.** Opening a segment allocates its frame. When the
awaiting coroutine is a `task`, its awaiter is `noexcept`, so a failed
allocation calls `std::terminate`.

## Cost

When no stop is requested:

- one segment frame per segment, and one gate frame per quitter that
  `run_async` or `run(ex)` starts. Both are allocated from capy's
  process-wide recycling memory resource rather than the frame
  allocator, because a teardown can free them after the chain that
  owns the frame allocator is gone;
- one extra resume per `co_await` that suspends, through the gate, and
  one at launch, when `run_async` or `run(ex)` starts the quitter
  through its gate;
- a stop-token check at every `co_await`, before the operation starts;
- nothing more at a completion. Only while a stop is pending is the
  result taken into the awaiter, checked, and then moved to the body:
  two moves more than in a `task`;
- a larger object: `sizeof(quitter<T>)` is 32 bytes on 64-bit targets,
  against 16 for `task<T>`. A quitter also holds the handle of the
  segment frame it opened, a function pointer used to unwind it, and a
  second flag.

`bench/quitter.cpp` compares `quitter` with `task`, including the
synchronous `results<>` completion. It prints whether
`BOOST_CAPY_SYMMETRIC_TRANSFER_WORKAROUND` is on, since that changes
how every `co_await` suspends.

## Usage

### Basic

```cpp
quitter<> session(tcp_socket& sock)
{
    char buf[1024];
    auto [ec, n] = co_await sock.read_some(buf);
    // A stop ends the quitter at the co_await above. We get here
    // after a stop only with a result the read had already produced
    // (see Completions).
    if (ec)
        co_return;
    co_await process(buf, n);
}
```

### Application shutdown

`example/quitter-shutdown` runs four `quitter<>` workers with no
cancellation code. A Ctrl+C requests the stop: every worker ends at the
`co_await` it is waiting in, its RAII guard runs, and its handler is
called.

### Mixing with `task<T>`

```cpp
quitter<results<>> some_quitter_function();

task<> outer()
{
    // If the quitter is stopped, ec is cond::canceled. The task
    // decides what to do with it.
    auto [ec] = co_await some_quitter_function();
    if (ec == cond::canceled)
        co_return;
}
```

A `task<T>` keeps explicit error handling: it sees the stop as a
value, and its own code decides. `quitter<std::error_code>` works the
same way. `when_all` and `when_any` take only `results` children.

## Design Decisions

**Why a separate type instead of a flag on `task<T>`?**

The task type is a policy point. Users who want explicit error
handling use `task<T>`. Users who want transparent cancellation
use `quitter<T>`. The ContextAwaitable protocol does not mandate a
task. This is the design P4003 enables.

**Why abandonment instead of an internal exception?**

The first implementation threw a sentinel exception at the next resume
point and caught it in `unhandled_exception`. That design had four
problems:

- any `catch(...)` around a `co_await` could intercept the stop;
- every quitter frame was resumed only to throw, and every frame above
  it rethrew;
- it could not be used without exceptions;
- a throw after the inner `await_ready` had acquired a resource skipped
  the `await_resume` that hands it over, so a stop could leak
  `async_mutex`'s lock.

Abandonment (`capy::abandon`, `chain_owner`) already ends a suspended
chain by destroying its frames from the top down. A stop now uses the
same cleanup, and only the gate decides between resuming and
abandoning.

**Why a gate instead of checking in `await_resume`?**

By the time `await_resume` runs, the frame has been resumed, and the
only way back out is to throw. The gate decides before any quitter
frame resumes.

**Why check the stop before the inner `await_ready`?**

A pending stop then starts no work. Checking later would let an
operation start, and possibly acquire something, that the body would
then receive after the stop.

**Why does the operation decide, and not the stop?**

An operation can complete in the window between the stop request and
the quitter's check: a read has its bytes, or a mutex has handed over
its lock. Abandoning then would discard a real result. For a lock, the
awaitable's destructor would have to give it back, which turns a
teardown-only duty of every awaitable into one that every stop
exercises. Letting the result decide keeps the stop invisible to the
body: it receives either a real result or nothing, never a `canceled`
it has to handle. This is how a sender's operation chooses between
`set_value` and `set_stopped`.

**Why can't a `task` await a `quitter<>`?**

A `void` return would read as success. In
`co_await save(); co_await notify_saved();`, the second line would run
even when `save()` was stopped. The stop needs a value that can say
so, and only `results` and `std::error_code` have one. Throwing it
instead would make a stop an exception in task code: a
`catch(std::exception&)` in a retry loop would catch every shutdown
and spin, and a handler that rethrows a copy of the stop would end
the program. Inside the quitter world no value is needed: a quitter
awaiting a quitter shares its segment, and both end together.

**Why does the stop have a private exception type?**

`run_async` reports the stop of a `quitter<>` or plain-`T` quitter to
its error handler, and its default handler drops it. But it must not
drop a real `ECANCELED` that a task throws. The stop is thrown as a
private type derived from `std::system_error`, so an error handler
still catches it as one, and the default handler drops only that
type.

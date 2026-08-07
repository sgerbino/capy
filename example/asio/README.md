# Asio Integration Examples

This directory demonstrates how to use Capy with Boost.Asio, showcasing three complementary integration patterns:

- **Writing portable stream algorithms** that hide Asio entirely
- **Adapting Capy streams** to work with Asio's Universal Async Model
- **Using Asio operations directly** from Capy coroutines via `use_capy`

---

## any_stream.cpp

This example shows how to write stream algorithms that have **zero knowledge of Asio**. The `reader` and `writer` functions take `capy::any_stream&` as their only dependency, using `co_await` with Capy's awaitable interface.

```cpp
capy::task<>
writer(capy::any_stream& stream, std::size_t total)
{
    char buf[128];
    // ...
    auto [ec, n] = co_await stream.write_some(capy::make_buffer(buf, chunk));
    // ...
}

capy::task<>
reader(capy::any_stream& stream, std::size_t total)
{
    char buf[128];
    // ...
    auto [ec, n] = co_await stream.read_some(capy::make_buffer(buf));
    // ...
}
```

Notice that the algorithm includes no Asio headers and uses no Asio types. Asio only appears in `main()` where the infrastructure is set up:

```cpp
int main()
{
    asio_context ctx;
    auto [s1, s2] = make_stream_pair(ctx);
    capy::run_async(ctx.get_executor())( run_example(s1, s2) );
    ctx.run();
}
```

This demonstrates how library authors can write stream algorithms that work with any I/O backend. The algorithms are completely portable and can be reused regardless of the underlying transport.

---

## asio_callbacks.cpp

This example shows that Capy's type-erased streams can participate in Asio's ecosystem through a thin adapter. The `uni_stream` wrapper implements Asio's **Universal Async Model** on top of `any_stream`.

Because it implements the Universal Async Model, `uni_stream` works with **any completion token**:

- Callbacks
- Stackful coroutines
- Stackless coroutines
- Fauxroutines
- Futures
- User-defined completion tokens

The example uses callbacks, but the same `uni_stream` would work identically with any other completion token type.

```cpp
net::async_write(
    state_->stream,
    net::buffer(state_->buf, chunk),
    [self = *this](boost::system::error_code ec, std::size_t n) mutable
    {
        // ...
    });
```

Standard Asio free functions like `net::async_read` and `net::async_write` work directly with `uni_stream`. The setup is straightforward:

```cpp
int main()
{
    asio_context ctx;
    auto [client, server] = make_uni_pair(ctx);
    do_write(client, total_bytes)();
    do_read(server, total_bytes)();
    ctx.run();
}
```

---

## use_capy_example.cpp

This example demonstrates the `use_capy` completion token, which allows **any Asio async operation** to be awaited directly from a Capy coroutine. The returned awaitable satisfies Capy's `IoAwaitable` concept with the extended `await_suspend` signature.

```cpp
capy::task<>
writer(net::ip::tcp::socket& socket, std::size_t total)
{
    char buf[128];
    // ...
    // Use Asio's async_write_some with use_capy completion token
    auto [ec, n] = co_await socket.async_write_some(
        net::buffer(buf, chunk), use_capy);
    // ...
}

capy::task<>
reader(net::ip::tcp::socket& socket, std::size_t total)
{
    char buf[128];
    // ...
    // Use Asio's async_read_some with use_capy completion token
    auto [ec, n] = co_await socket.async_read_some(
        net::buffer(buf), use_capy);
    // ...
}
```

The `use_capy` token:

- Works with any Asio async operation
- Returns an `IoAwaitable` that integrates with Capy's executor and cancellation
- Bridges `std::stop_token` to Asio's cancellation system
- Returns results as `capy::io_result<...>` for structured bindings
- Uses `asio::deferred_async_operation` internally (no `std::function` overhead)

---

## executor_bridge.cpp

This example is the reverse of the others: instead of running Capy on
Asio's loop, it gives Asio code a home on a Capy scheduler. The
`asio_executor` in `api/asio_executor.hpp` is an Asio standard
executor that submits every callable to a `capy::Executor` as a
synthetic-frame `capy::continuation` (the "universal continuation"
technique of P4126R0) — no coroutine frame, and no changes to any
Capy executor.

```cpp
capy::thread_pool pool(4);
bridge_context ctx(pool.get_executor());

net::post(ctx.get_executor(), []{ /* runs on the pool */ });
auto strand = net::make_strand(ctx.get_executor());

bridge_timer timer(ctx.get_executor(), 10ms);
timer.async_wait([](auto ec){ /* completes on the pool */ });
```

Because the executor satisfies Asio's standard executor requirements
(including the `blocking`, `relationship`, and `outstanding_work`
properties and the `context` query), the whole Asio ecosystem
follows: `post`/`dispatch`/`defer`, `asio::strand`, work guards, and
I/O objects. `bridge_context` owns a hidden `io_context` and pump
thread where reactors and services live; completion handlers never
run there — they dispatch through the bridge executor onto the Capy
scheduler.

A platform that rejects the synthetic-frame technique could instead
post a tiny real coroutine per callable, at the cost of a coroutine
frame per submission.

## Which direction do I need?

| Host application | Home loop | Piece | Example |
|---|---|---|---|
| Asio-centric, adopting Capy tasks | `asio::io_context` | `asio_context` + `use_capy` | `any_stream.cpp`, `use_capy_example.cpp` |
| Capy/corosio-centric, consuming Asio code | Capy scheduler | `bridge_context` + `asio_executor` | `executor_bridge.cpp` |

---

## Key Takeaways

- **any_stream approach**: Write algorithms once, run them on any I/O backend. Complete portability with no Asio dependency in algorithm code.

- **uni_stream approach**: Implement Asio's Universal Async Model on Capy streams. Full compatibility with Asio's completion token system, enabling use of existing Asio utilities and patterns.

- **use_capy approach**: Call Asio async operations directly from Capy coroutines. Useful when you need access to Asio-specific features or want to integrate existing Asio code into Capy applications.

- **executor_bridge approach**: Run Asio-ecosystem code (handlers,
  strands, work guards, timers, sockets) on a Capy scheduler via an
  Asio-shaped executor. The cost lives entirely in the bridge; the
  native Capy path is unchanged.

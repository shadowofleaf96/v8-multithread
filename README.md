V8 JavaScript Engine
=============

V8 is Google's open source JavaScript engine.

V8 implements ECMAScript as specified in ECMA-262.

V8 is written in C++ and is used in Chromium, the open source
browser from Google.

V8 can run standalone, or can be embedded into any C++ application.

V8 Project page: https://v8.dev/docs


Getting the Code
=============

Checkout [depot tools](http://www.chromium.org/developers/how-tos/install-depot-tools), and run

        fetch v8

This will checkout V8 into the directory `v8` and fetch all of its dependencies.
To stay up to date, run

        git pull origin
        gclient sync

For fetching all branches, add the following into your remote
configuration in `.git/config`:

        fetch = +refs/branch-heads/*:refs/remotes/branch-heads/*
        fetch = +refs/tags/*:refs/tags/*


Multithreading Support
=============

V8 now includes **experimental multithreading support** that enables true parallel
JavaScript execution across multiple OS threads. The design is inspired by Rust's
threading model — safe concurrency through ownership and message passing — while
staying idiomatic to JavaScript with full **async/await** integration.

### Architecture Overview

- **Work-stealing thread pool**: A fixed-size pool of OS threads (defaults to
  `navigator.hardwareConcurrency`). Idle threads steal tasks from busy ones for
  optimal load balancing.
- **Isolate-per-pool-thread**: Each pool thread owns a pre-warmed V8 Isolate,
  keeping memory usage fixed and avoiding per-spawn overhead.
- **Shared-nothing memory model**: Threads do not share mutable state. Data
  moves between threads via structured cloning (deep copy), transferables (move
  semantics), or `SharedArrayBuffer` (zero-copy for advanced use).
- **Async-first design**: Every blocking operation returns a `Promise`, so threads
  integrate naturally with `async/await` and the event loop.

### JavaScript API

For a simplified and fully explained guide of the APIs, see the [Multithreading API Documentation](MULTITHREADING_API.md).

#### Thread.spawn — Async Task Execution

`Thread.spawn` schedules a function on a pool thread and returns an awaitable
`JoinHandle`:

```js
// Basic spawn + await
const handle = Thread.spawn(() => {
  return fibonacci(40);
});
const result = await handle.join(); // 102334155

// Spawn with arguments (serialized via structured clone)
const handle = Thread.spawn((a, b) => {
  return a * b;
}, 6, 7);
const result = await handle.join(); // 42

// Async function inside a thread
const handle = Thread.spawn(async () => {
  const data = await fetchData();     // each thread has its own event loop
  return processData(data);
});
const result = await handle.join();
```

#### Thread.sleep — Non-blocking Sleep

```js
async function delayedWork() {
  console.log("Starting...");
  await Thread.sleep(1000);           // sleeps 1 second, non-blocking
  console.log("Done!");
}
```

#### Channels — Async Message Passing (like Rust's `mpsc`)

Channels provide safe cross-thread communication with async/await:

```js
const [tx, rx] = Thread.channel();

// Producer thread
Thread.spawn(async () => {
  for (let i = 0; i < 10; i++) {
    await tx.send({ index: i, value: i * i });
    await Thread.sleep(100);
  }
  tx.close();                         // signal no more messages
});

// Consumer — async iteration
for await (const msg of rx) {
  console.log(msg);                   // { index: 0, value: 0 }, ...
}

// Or receive one at a time
const msg = await rx.recv();          // awaits next message
```

#### Mutex — Async-Safe Shared State (like Rust's `Mutex<T>`)

```js
const counter = Thread.mutex(0);      // Mutex<number>

// Spawn 10 threads, each incrementing the counter
const handles = Array.from({ length: 10 }, () =>
  Thread.spawn(async () => {
    for (let i = 0; i < 1000; i++) {
      await counter.lock(value => value + 1);   // async lock + transform
    }
  })
);

// Await all threads
await Promise.all(handles.map(h => h.join()));
console.log(await counter.value());  // 10000 — no data races
```

#### Async/Await Patterns — Full Integration

Threads are first-class async citizens. Every thread API returns a `Promise`,
so they compose naturally with existing async patterns:

```js
// Parallel async computations
async function processAll(items) {
  const handles = items.map(item =>
    Thread.spawn(async () => {
      const result = await heavyCompute(item);
      return result;
    })
  );
  // Await all results — runs truly in parallel, not just concurrent
  return Promise.all(handles.map(h => h.join()));
}

// Try/catch works across threads
try {
  const handle = Thread.spawn(() => {
    throw new Error("Thread error!");
  });
  await handle.join();
} catch (e) {
  console.error(e.message);          // "Thread error!" — propagated
}

// Race between threads
const fastest = await Promise.race([
  Thread.spawn(() => computeRouteA(data)).join(),
  Thread.spawn(() => computeRouteB(data)).join(),
]);

// AbortController integration
const controller = new AbortController();
const handle = Thread.spawn(async (signal) => {
  while (!signal.aborted) {
    await doWork();
  }
}, { signal: controller.signal });
// Later: controller.abort();
```

#### Under the Hood: Truly Non-Blocking `await`
When you `await` a thread operation (like `handle.join()` or `tx.send()`), it **does not block the underlying OS thread**. Instead:
1. It yields the current V8 `Isolate` execution back to the event loop.
2. The OS thread is immediately freed and returned to the work-stealing pool to execute other pending tasks.
3. When the awaited operation completes in the background, your JS task is re-queued and resumes execution.

This means you can spawn 100,000 threads with `Thread.spawn` and `await` them all, and it will only ever consume a small number of actual OS threads (equal to your pool size).

### Automatic Parallelism

When the engine detects independent work, it automatically distributes across
the thread pool — no API changes needed:

```js
// Promise.all — independent promises run on separate pool threads
const [users, orders, analytics] = await Promise.all([
  fetchUsers(),
  fetchOrders(),
  computeAnalytics(),
]);

// Array.parallelMap — data parallelism across threads
const results = await [1, 2, 3, 4, 5, 6, 7, 8]
  .parallelMap(async (n) => {
    return await heavyTransform(n);   // each runs on a pool thread
  });

// Array.parallelFilter
const valid = await data.parallelFilter(async (item) => {
  return await expensiveValidation(item);
});

// Array.parallelReduce — tree-based parallel reduction
const total = await numbers.parallelReduce(async (a, b) => a + b, 0);
```

### Building with Multithreading

Multithreading is opt-in. Enable it with the `v8_enable_multithreading` GN flag:

```bash
# Generate build files with multithreading enabled
gn gen out/x64.release --args='v8_enable_multithreading=true'

# Build
ninja -C out/x64.release d8

# Run a script using threads
out/x64.release/d8 --enable-multithreading my_script.js
```

#### Build Flags

| Flag | Default | Description |
|------|---------|-------------|
| `v8_enable_multithreading` | `false` | Enable the threading runtime and JS API |
| `v8_thread_pool_size` | `0` (auto) | Number of pool threads. `0` = `hardware_concurrency` |

### Node.js Integration

> [!TIP]
> **Want to test it out?** You can use the pre-configured custom Node.js repository ready for testing: [shadowofleaf96/custom-node](https://github.com/shadowofleaf96/custom-node).

This experimental multithreading engine can be embedded directly into Node.js, allowing native multithreading in your Node.js applications.

To build Node.js with V8 multithreading support:

1. Clone the Node.js repository (`git clone https://github.com/nodejs/node.git`).
2. Replace the `deps/v8` directory in the Node.js source tree with this customized V8 repository.
3. Configure the Node.js build with the multithreading flag enabled:
   ```bash
   # On Windows (requires Visual Studio with C++ Clang Compiler and Rust)
   .\vcbuild.bat --enable-v8-multithreading
   
   # On POSIX (Linux/macOS)
   ./configure --enable-v8-multithreading
   make -j8
   ```
4. This will automatically compile V8's multithreading components and link them into the Node.js binary. The threading APIs (`Thread.spawn`, `Thread.channel`, etc.) will be exposed natively within the Node.js environment.

### Platform Support

| Platform | Architecture | Status |
|----------|-------------|--------|
| Linux    | x64, arm64  | ✅ Supported |
| macOS    | x64, arm64  | ✅ Supported |
| Windows  | x64         | ✅ Supported |

### Design Principles

1. **Safety by default** — No shared mutable state. Data races are impossible
   without explicit opt-in (`SharedArrayBuffer`).
2. **Zero-cost when unused** — Multithreading is behind a build flag. No runtime
   overhead when disabled.
3. **Async-native** — Every thread operation is a `Promise`. No callback hell,
   no blocking the event loop.
4. **Rust-inspired, JS-idiomatic** — Familiar API patterns from Rust's `std::thread`,
   `std::sync::mpsc`, and `std::sync::Mutex`, but adapted for JavaScript's
   async/await ecosystem.


Contributing
=============

Please follow the instructions mentioned at
[v8.dev/docs/contribute](https://v8.dev/docs/contribute).

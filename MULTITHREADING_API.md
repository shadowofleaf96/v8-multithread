# V8 Multithreading API Documentation

This document provides a simplified and easy-to-understand explanation of the new experimental multithreading APIs introduced in V8. These APIs allow you to run JavaScript code in parallel across multiple OS threads, making it possible to utilize modern multi-core processors efficiently without blocking the main event loop.

## 1. Thread Execution

### `Thread.spawn(callback, ...args)`
Spawns a new task on a background thread from the work-stealing pool.
- **`callback`**: The function to execute on the new thread. It can be a regular function or an `async` function.
- **`...args`**: Optional arguments to pass to the callback. These arguments are transferred safely to the new thread via structured cloning.
- **`options`**: (Optional) As the very last argument, you can pass an object `{ transfer: [buffer1, ...] }` to perform zero-copy transfer of `ArrayBuffer`s, avoiding expensive memory cloning.
- **Returns**: A `JoinHandle` representing the background task.

**Example:**
```javascript
const handle = Thread.spawn((a, b) => {
  return a + b;
}, 10, 20);

// Zero-copy transfer of a large ArrayBuffer
const largeBuffer = new ArrayBuffer(1024 * 1024 * 10); // 10MB
const transferHandle = Thread.spawn((buffer) => {
  console.log("Buffer received:", buffer.byteLength);
}, largeBuffer, { transfer: [largeBuffer] });
```

### `Thread.join(handle)`
Waits for a thread to finish its execution and retrieves its return value.
- **`handle`**: The `JoinHandle` returned by `Thread.spawn()`.
- **Returns**: A `Promise` that resolves with the return value of the spawned thread, or rejects if the thread throws an error.

**Example:**
```javascript
const result = await Thread.join(handle);
console.log(result); // 30
```

### `Thread.sleep(ms)`
Pauses the current thread's execution for a specified amount of time. Crucially, this is non-blocking: it yields execution back to the event loop, freeing the underlying OS thread for other tasks.
- **`ms`**: The number of milliseconds to sleep.
- **Returns**: A `Promise` that resolves when the time has elapsed.

**Example:**
```javascript
console.log("Waiting...");
await Thread.sleep(1000);
console.log("1 second passed!");
```

---

## 2. Channels (Message Passing)

Channels allow safe, asynchronous communication between different threads, similar to message passing in Rust or Go. Data is safely cloned between threads to prevent data races.

### `Thread.channel(capacity?)`
Creates a new communication channel.
- **`capacity`**: (Optional) The maximum number of pending messages the channel can hold. If specified, the channel applies **back-pressure**: sending a message when the channel is full will pause the sender's `Promise` until a receiver consumes a message.
- **Returns**: An array containing two endpoints: `[tx, rx]`. 
  - `tx` (Transmitter): Used to send messages.
  - `rx` (Receiver): Used to receive messages.

**Example:**
```javascript
const [tx, rx] = Thread.channel(); // Unbounded channel
const [boundedTx, boundedRx] = Thread.channel(100); // Back-pressure applied after 100 messages
```

### `tx.send(message, transferList?)`
Sends a message into the channel to be received by another thread.
- **`message`**: The data to send.
- **`transferList`**: (Optional) An array of `ArrayBuffer` objects to transfer to the receiver without copying the underlying memory.
- **Returns**: A `Promise` that resolves when the message is successfully sent (or queued if there is capacity).

**Example:**
```javascript
await tx.send({ user: "Alice", id: 1 });

// Zero-copy transfer via channel
const buffer = new ArrayBuffer(1024);
await tx.send({ data: buffer }, [buffer]);
```

### `rx.recv()`
Waits for and receives the next message from the channel.
- **Returns**: A `Promise` that resolves with the received message.

**Example:**
```javascript
const message = await rx.recv();
console.log(message.user); // "Alice"
```

---

## 3. Mutex (Shared State)

A Mutex (Mutual Exclusion) allows multiple threads to safely read and update shared data without conflicts, race conditions, or explicit locks.

### `Thread.mutex(initialValue)`
Creates a new Mutex protecting a shared value.
- **`initialValue`**: The initial state of the data to protect.
- **Returns**: A `Mutex` object.

**Example:**
```javascript
const mutex = Thread.mutex(0);
```

### `mutex.lock(callback)`
Asynchronously acquires exclusive access to the shared value, runs the callback to modify it, and then releases the lock.
- **`callback(currentValue)`**: A function that receives the current value and returns the new value.
- **Returns**: A `Promise` that resolves with the new value once the lock is released.

**Example:**
```javascript
await mutex.lock((currentValue) => {
  return currentValue + 1;
});
```

### `mutex.value()`
Reads the current value of the mutex synchronously without modifying it.
- **Returns**: The current shared value.

**Example:**
```javascript
const currentValue = mutex.value();
console.log(currentValue); // 1
```

---

## 4. Automatic Parallelism

V8 can automatically execute certain independent tasks in parallel using background threads without requiring explicit manual thread management.

### `Array.prototype.parallelMap(callback)`
Works like standard `Array.prototype.map()`, but processes the elements concurrently across multiple threads in the pool.
- **Returns**: A `Promise` resolving to an array of the mapped results.

**Example:**
```javascript
const numbers = [1, 2, 3, 4];
const doubled = await numbers.parallelMap(n => n * 2);
console.log(doubled); // [2, 4, 6, 8]
```

### `Array.prototype.parallelFilter(callback)`
Works like standard `Array.prototype.filter()`, but evaluates the elements concurrently across multiple threads.
- **Returns**: A `Promise` resolving to an array of the elements that pass the filter.

**Example:**
```javascript
const numbers = [1, 2, 3, 4, 5];
const evens = await numbers.parallelFilter(n => n % 2 === 0);
console.log(evens); // [2, 4]
```

### `Array.prototype.parallelReduce(callback, initialValue)`
Works like standard `Array.prototype.reduce()`, but operates in a tree-based parallel reduction map-reduce style across multiple threads, dramatically speeding up cumulative operations.
- **`callback(accumulator, currentValue)`**: A function to execute on each element in the array.
- **`initialValue`**: (Optional) A value to use as the first argument to the first call of the callback.
- **Returns**: A `Promise` resolving to the final accumulated value.

**Example:**
```javascript
const numbers = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
const sum = await numbers.parallelReduce((a, b) => a + b, 0);
console.log(sum); // 55
```

### `Promise.all(promises)`
When using `Promise.all()` with multiple promises, V8 will automatically execute their subsequent `.then()` reactions in parallel if the reaction handlers are pure (e.g., they only use their arguments and don't capture outer variables in closures).

**Example:**
```javascript
const p1 = Promise.resolve(10);
const p2 = Promise.resolve(20);

// These reactions will execute in parallel on background threads
const results = await Promise.all([
  p1.then(x => x * 2),
  p2.then(x => x + 5)
]);
console.log(results); // [20, 25]
```

---

## 5. Thread Pool Management

You can dynamically adjust the size of the underlying thread pool at runtime to match your workload or reclaim memory.

### `Thread.getPoolSize()`
Gets the current number of active threads in the work-stealing pool.
- **Returns**: The number of threads.

**Example:**
```javascript
const size = Thread.getPoolSize();
console.log(`Currently using ${size} threads.`);
```

### `Thread.setPoolSize(size)`
Dynamically scales the thread pool up or down. If scaling down, excess threads are gracefully terminated once they finish their current task, freeing their memory.
- **`size`**: The desired number of threads (between 1 and 128).

**Example:**
```javascript
// Temporarily boost thread count for heavy processing
Thread.setPoolSize(16);
await heavyParallelTask();

// Reclaim memory by shrinking the pool back down
Thread.setPoolSize(4);
```

# V8 Multithreading API Documentation

This document provides a simplified and easy-to-understand explanation of the new experimental multithreading APIs introduced in V8. These APIs allow you to run JavaScript code in parallel across multiple OS threads, making it possible to utilize modern multi-core processors efficiently without blocking the main event loop.

## 1. Thread Execution

### `Thread.spawn(callback, ...args)`
Spawns a new task on a background thread from the work-stealing pool.
- **`callback`**: The function to execute on the new thread. It can be a regular function or an `async` function.
- **`...args`**: Optional arguments to pass to the callback. These arguments are transferred safely to the new thread via structured cloning.
- **Returns**: A `JoinHandle` representing the background task.

### `Thread.join(handle)`
Waits for a thread to finish its execution and retrieves its return value.
- **`handle`**: The `JoinHandle` returned by `Thread.spawn()`.
- **Returns**: A `Promise` that resolves with the return value of the spawned thread, or rejects if the thread throws an error.

### `Thread.sleep(ms)`
Pauses the current thread's execution for a specified amount of time. Crucially, this is non-blocking: it yields execution back to the event loop, freeing the underlying OS thread for other tasks.
- **`ms`**: The number of milliseconds to sleep.
- **Returns**: A `Promise` that resolves when the time has elapsed.

---

## 2. Channels (Message Passing)

Channels allow safe, asynchronous communication between different threads, similar to message passing in Rust or Go. Data is safely cloned between threads to prevent data races.

### `Thread.channel()`
Creates a new communication channel.
- **Returns**: An array containing two endpoints: `[tx, rx]`. 
  - `tx` (Transmitter): Used to send messages.
  - `rx` (Receiver): Used to receive messages.

### `tx.send(message)`
Sends a message into the channel to be received by another thread.
- **`message`**: The data to send.
- **Returns**: A `Promise` that resolves when the message is successfully sent.

### `rx.recv()`
Waits for and receives the next message from the channel.
- **Returns**: A `Promise` that resolves with the received message.

---

## 3. Mutex (Shared State)

A Mutex (Mutual Exclusion) allows multiple threads to safely read and update shared data without conflicts, race conditions, or explicit locks.

### `Thread.mutex(initialValue)`
Creates a new Mutex protecting a shared value.
- **`initialValue`**: The initial state of the data to protect.
- **Returns**: A `Mutex` object.

### `mutex.lock(callback)`
Asynchronously acquires exclusive access to the shared value, runs the callback to modify it, and then releases the lock.
- **`callback(currentValue)`**: A function that receives the current value and returns the new value.
- **Returns**: A `Promise` that resolves with the new value once the lock is released.

### `mutex.value()`
Reads the current value of the mutex synchronously without modifying it.
- **Returns**: The current shared value.

---

## 4. Automatic Parallelism

V8 can automatically execute certain independent tasks in parallel using background threads without requiring explicit manual thread management.

### `Array.prototype.parallelMap(callback)`
Works like standard `Array.prototype.map()`, but processes the elements concurrently across multiple threads in the pool.
- **Returns**: A `Promise` resolving to an array of the mapped results.

### `Array.prototype.parallelFilter(callback)`
Works like standard `Array.prototype.filter()`, but evaluates the elements concurrently across multiple threads.
- **Returns**: A `Promise` resolving to an array of the elements that pass the filter.

### `Promise.all(promises)`
When using `Promise.all()` with multiple promises, V8 will automatically execute their subsequent `.then()` reactions in parallel if the reaction handlers are pure (e.g., they only use their arguments and don't capture outer variables in closures).

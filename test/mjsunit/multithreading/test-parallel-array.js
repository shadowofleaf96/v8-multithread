// Copyright 2026 the V8 project authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Flags: --enable-multithreading

(async function testParallelMapBasic() {
  const arr = [1, 2, 3, 4, 5];
  const promise = arr.parallelMap(x => x * 2);
  const result = await promise;
  assertEquals(5, result.length);
  assertEquals(2, result[0]);
  assertEquals(4, result[1]);
  assertEquals(6, result[2]);
  assertEquals(8, result[3]);
  assertEquals(10, result[4]);
})();

(async function testParallelFilterBasic() {
  const arr = [1, 2, 3, 4, 5, 6];
  const promise = arr.parallelFilter(x => x % 2 === 0);
  const result = await promise;
  assertEquals(3, result.length);
  assertEquals(2, result[0]);
  assertEquals(4, result[1]);
  assertEquals(6, result[2]);
})();

(async function testParallelMapWithPrimitivesAndObjects() {
  const [tx, rx] = Thread.channel();
  const mutex = Thread.mutex(50);
  
  const arr = [tx, mutex];
  const promise = arr.parallelMap(item => {
    // If it's a channel, send a message
    if (item && typeof item.send === 'function') {
      item.send("sent_from_map");
      return "channel_done";
    }
    // If it's a mutex, return a value
    if (item && typeof item.lock === 'function') {
      return "mutex_done";
    }
    return "unknown";
  });
  
  const results = await promise;
  assertEquals(2, results.length);
  assertEquals("channel_done", results[0]);
  assertEquals("mutex_done", results[1]);
  
  // Verify channel message was received
  const msg = await rx.recv();
  assertEquals("sent_from_map", msg);
})();

(async function testParallelMapExceptions() {
  const arr = [1, 2, 3];
  try {
    await arr.parallelMap(x => {
      if (x === 2) {
        throw new Error("Failure in map callback");
      }
      return x;
    });
    assertUnreachable("Should have thrown");
  } catch (err) {
    assertEquals("Failure in map callback", err.message);
  }
})();

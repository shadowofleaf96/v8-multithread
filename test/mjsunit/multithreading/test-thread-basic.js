// Copyright 2026 the V8 project authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Flags: --enable-multithreading

(async function testBasic() {
  // Test spawn and join with return value
  const handle = Thread.spawn(() => {
    return 42;
  });
  const result = await Thread.join(handle);
  assertEquals(42, result);
})();

(async function testSpawnArgs() {
  // Test spawn passing arguments
  const handle = Thread.spawn((x, y, z) => {
    return x + y + z;
  }, 10, 20, 30);
  const result = await Thread.join(handle);
  assertEquals(60, result);
})();

(async function testSleep() {
  // Test sleep delay
  const start = Date.now();
  await Thread.sleep(100);
  const elapsed = Date.now() - start;
  assertTrue(elapsed >= 90, "Elapsed: " + elapsed);
})();

(async function testSpawnError() {
  // Test throwing error inside spawn
  const handle = Thread.spawn(() => {
    throw new Error("Task failed successfully");
  });
  try {
    await Thread.join(handle);
    assertUnreachable("Should have thrown");
  } catch (err) {
    assertEquals("Task failed successfully", err.message);
  }
})();

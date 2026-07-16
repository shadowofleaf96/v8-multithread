// Copyright 2026 the V8 project authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Flags: --enable-multithreading

(async function testMutexBasic() {
  const mutex = Thread.mutex(10);
  assertEquals(10, mutex.value());

  const result = await mutex.lock((val) => {
    return val + 5;
  });

  assertEquals(15, result);
  assertEquals(15, mutex.value());
})();

(async function testMutexContention() {
  const mutex = Thread.mutex(0);

  // Lock and increment concurrently
  const promises = [];
  for (let i = 0; i < 10; ++i) {
    promises.push(
      Thread.spawn((mut) => {
        return mut.lock((val) => val + 1);
      }, mutex).then(h => Thread.join(h))
    );
  }

  await Promise.all(promises);
  assertEquals(10, mutex.value());
})();

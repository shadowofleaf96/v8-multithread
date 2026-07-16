// Copyright 2026 the V8 project authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Flags: --enable-multithreading

(async function testPromiseAllParallelBasic() {
  const p1 = Promise.resolve(10);
  const p2 = Promise.resolve(20);
  const p3 = Promise.resolve(30);

  // Handlers must not have closures (they only use top-level context / arguments)
  const r1 = p1.then(x => x * 2);
  const r2 = p2.then(x => x + 5);
  const r3 = p3.then(x => x - 3);

  const results = await Promise.all([r1, r2, r3]);
  assertEquals(3, results.length);
  assertEquals(20, results[0]);
  assertEquals(25, results[1]);
  assertEquals(27, results[2]);
})();

(async function testPromiseAllParallelRejection() {
  const p1 = Promise.resolve(10);
  const p2 = Promise.resolve(20);

  const r1 = p1.then(x => { throw new Error("Rejection in reaction"); });
  const r2 = p2.then(x => x * 2);

  try {
    await Promise.all([r1, r2]);
    assertUnreachable("Should have rejected");
  } catch (err) {
    assertEquals("Rejection in reaction", err.message);
  }
})();

(async function testPromiseAllParallelWithChannelAndMutex() {
  const [tx, rx] = Thread.channel();
  const mutex = Thread.mutex(10);

  const p1 = Promise.resolve(tx);
  const p2 = Promise.resolve(mutex);

  // Handlers receive the resolved value (which are channel/mutex wrappers)
  const r1 = p1.then(item => {
    item.send("sent_from_promise_reaction");
    return "promise_channel_ok";
  });

  const r2 = p2.then(mut => {
    return mut.lock(val => val + 15);
  });

  const results = await Promise.all([r1, r2]);
  assertEquals(2, results.length);
  assertEquals("promise_channel_ok", results[0]);
  assertEquals(25, results[1]);

  const msg = await rx.recv();
  assertEquals("sent_from_promise_reaction", msg);
  assertEquals(25, mutex.value());
})();

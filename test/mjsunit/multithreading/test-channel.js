// Copyright 2026 the V8 project authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Flags: --enable-multithreading

(async function testChannelBasic() {
  const [tx, rx] = Thread.channel();

  tx.send({ hello: "world", count: 42 });
  const msg = await rx.recv();
  assertEquals("world", msg.hello);
  assertEquals(42, msg.count);
})();

(async function testChannelCrossThread() {
  const [tx, rx] = Thread.channel();

  // Spawn a thread that sends a message back
  Thread.spawn((sender) => {
    sender.send("Message from worker!");
  }, tx);

  const msg = await rx.recv();
  assertEquals("Message from worker!", msg);
})();

(async function testChannelMultipleProducers() {
  const [tx, rx] = Thread.channel();

  // Spawn multiple threads sending messages
  const h1 = Thread.spawn((sender) => { sender.send("A"); }, tx);
  const h2 = Thread.spawn((sender) => { sender.send("B"); }, tx);

  await Thread.join(h1);
  await Thread.join(h2);

  const m1 = await rx.recv();
  const m2 = await rx.recv();

  // Order may vary but both should be received
  const results = [m1, m2];
  assertTrue(results.includes("A"));
  assertTrue(results.includes("B"));
})();

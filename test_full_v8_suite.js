// Comprehensive V8 Multithreading Test Suite

console.log("=== 1. Basic Spawn & Join ===");
const handle = Thread.spawn((a, b) => {
  return a + b;
}, 10, 32);
const sum = await Thread.join(handle);
console.log("Thread.spawn result =", sum, sum === 42 ? "[OK]" : "[FAIL]");

console.log("\n=== 2. Thread.sleep ===");
const startTime = Date.now();
await Thread.sleep(200);
const elapsed = Date.now() - startTime;
console.log("Thread.sleep elapsed =", elapsed, "ms", elapsed >= 180 ? "[OK]" : "[FAIL]");

console.log("\n=== 3. Channels (mpsc) ===");
const [tx, rx] = Thread.channel(5);
Thread.spawn(async (sender) => {
  for (let i = 1; i <= 3; i++) {
    await sender.send(i * 10);
  }
  sender.close();
}, tx);

const channelResults = [];
for await (const msg of rx) {
  channelResults.push(msg);
}
console.log("Channel results =", channelResults, JSON.stringify(channelResults) === "[10,20,30]" ? "[OK]" : "[FAIL]");

console.log("\n=== 4. Mutex (Shared State) ===");
const m = Thread.mutex(0);
const spawnHandles = Array.from({ length: 5 }, () =>
  Thread.spawn(async (mutex) => {
    for (let i = 0; i < 10; i++) {
      await mutex.lock((val) => val + 1);
    }
  }, m)
);
for (const h of spawnHandles) {
  await Thread.join(h);
}
const finalVal = await m.value();
console.log("Mutex final value =", finalVal, finalVal === 50 ? "[OK]" : "[FAIL]");

console.log("\n=== 5. Parallel Array Methods ===");
const numbers = [1, 2, 3, 4, 5];
const mapped = await numbers.parallelMap((x) => x * 2);
console.log("parallelMap =", mapped, JSON.stringify(mapped) === "[2,4,6,8,10]" ? "[OK]" : "[FAIL]");

const filtered = await numbers.parallelFilter((x) => x > 3);
console.log("parallelFilter =", filtered, JSON.stringify(filtered) === "[4,5]" ? "[OK]" : "[FAIL]");

const reduced = await numbers.parallelReduce((a, b) => a + b, 0);
console.log("parallelReduce =", reduced, reduced === 15 ? "[OK]" : "[FAIL]");

console.log("\n=== 6. SharedArrayBuffer (Zero-Copy) ===");
const sab = new SharedArrayBuffer(16);
const i32 = new Int32Array(sab);
i32[0] = 100;
const sabHandle = Thread.spawn((buf) => {
  const arr = new Int32Array(buf);
  arr[0] = 777;
}, sab);
await Thread.join(sabHandle);
console.log("SAB Main Thread value =", i32[0], i32[0] === 777 ? "[OK]" : "[FAIL]");

console.log("\n=== 7. Unified Stack Traces ===");
try {
  const errHandle = Thread.spawn(() => {
    throw new Error("Worker test error");
  });
  await Thread.join(errHandle);
} catch (err) {
  const hasCallSite = err.stack.includes("test_full_v8_suite.js");
  console.log("Captured Stack Trace:\n" + err.stack);
  console.log("Unified stack trace test =", hasCallSite ? "[OK]" : "[FAIL]");
}

console.log("\n=== ALL V8 MULTITHREADING TESTS COMPLETED ===");

// SAB and Stack Trace Test
const sab = new SharedArrayBuffer(1024);
const view = new Int32Array(sab);
view[0] = 42;

console.log("Main Thread: Initial SAB value =", view[0]);

const handle = Thread.spawn((sharedBuffer) => {
  // Test 1: Zero-copy SAB transfer
  const workerView = new Int32Array(sharedBuffer);
  if (workerView[0] !== 42) {
    throw new Error("Worker did not receive correct SAB data!");
  }
  
  // Modify the SAB directly
  workerView[0] = 99;
  
  // Test 2: Unified Stack Trace (intentional crash)
  throw new Error("Intentional worker crash to test stack traces!");
}, sab);

try {
  await Thread.join(handle);
} catch (err) {
  console.log("\n--- CAUGHT ERROR ---");
  console.log(err.stack);
  console.log("--------------------\n");
  
  console.log("Main Thread: Final SAB value =", view[0]);
  if (view[0] === 99) {
    console.log("SUCCESS: SharedArrayBuffer zero-copy modification worked!");
  } else {
    console.log("FAILED: SharedArrayBuffer modification failed!");
  }
  
  if (err.stack && (err.stack.includes("test_sab_stack.js") || err.stack.includes("Thread.spawn"))) {
    console.log("SUCCESS: Unified stack trace bridges the main thread!");
  } else {
    console.log("FAILED: Stack trace did not bridge threads.");
  }
}

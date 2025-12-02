const { createClient } = require("redis");
(async () => {
  // -----------------------
  // CONNECT
  // -----------------------
  const client = createClient({
    url: "redis://192.168.0.72:6379",
  });
  client.on("error", (err) => console.error("Redis Client Error", err));
  await client.connect();

  console.log("✅ Connected to Redis\n");

  // -----------------------
  // MEMORY TEST
  // -----------------------
  console.log("=== MEMORY TEST ===");
  await client.set("mem:test", "Hello Redis!");
  const info = await client.info("memory");
  console.log(
    info.split("\n").filter((l) => l.includes("used_memory_human"))[0]
  );

  // -----------------------
  // PUB/SUB TEST
  // -----------------------
  console.log("\n=== PUB / SUB TEST ===");

  const sub = client.duplicate();
  await sub.connect();

  await sub.subscribe("test-channel", (message) => {
    console.log("📨 Subscriber received:", message);
  });

  // Publish message
  await client.publish("test-channel", "Hello from publisher!");

  // Give subscriber time to receive message
  await new Promise((r) => setTimeout(r, 1000));

  await sub.unsubscribe("test-channel");
  await sub.quit();

  // -----------------------
  // STREAMS TEST
  // -----------------------
  console.log("\n=== STREAMS TEST ===");

  const streamKey = "test-stream";

  // Add event to a stream
  const id = await client.xAdd(streamKey, "*", {
    event: "user_login",
    user: "john",
    time: Date.now().toString(),
  });

  console.log("✅ XADD ID:", id);

  // Read from stream
  const entries = await client.xRead([{ key: streamKey, id: "0" }], {
    COUNT: 10,
    BLOCK: 1000,
  });

  if (entries) {
    for (const stream of entries) {
      for (const msg of stream.messages) {
        console.log("📖 XREAD MESSAGE:", msg.id, msg.message);
      }
    }
  }

  // -----------------------
  // CLEAN UP
  // -----------------------
  await client.quit();
  console.log("\n✅ Done.");
})();

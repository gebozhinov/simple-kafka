# Stage 2 – Protocol Layer Notes

Source: https://buildthingsuseful.medium.com/stage-2-building-your-own-kafka-creating-the-core-protocol-layer-5838c1b2364c
Code: `src/main/java/com/simplekafka/broker/Protocol.java`

## Why a protocol at all?

TCP only moves raw bytes. It has no idea what a "topic" or a "partition" is.
If a producer wants to tell the broker "store this message in topic X, partition Y",
both sides need to agree on how to lay that request out as bytes. That agreement
is the **protocol**.

- **encode** methods (client side) = **serialize**: Java objects → bytes
- **decode** methods (broker side) = **deserialize**: bytes → Java objects

Encode and decode must read/write the fields in **exactly the same order and sizes**,
otherwise the other side reads garbage.

---

## `encodeProduceRequest`(String topic, int partition, byte[] message)`

### Purpose
Serializes a "produce" request (send a message to topic/partition) into bytes
that can be written to a socket.

### Wire layout

| Field          | Type    | Size (bytes)  |
|----------------|---------|---------------|
| request type   | byte    | 1             |
| topic length   | short   | 2             |
| topic          | bytes   | N (variable)  |
| partition      | int     | 4             |
| message length | int     | 4             |
| message        | bytes   | M (variable)  |

Fixed part: `1 + 2 + 4 + 4 = 11` → that's where `allocate(11 + topic + message)` comes from.

### Example
`encodeProduceRequest("orders", 0, "hi".getBytes())` → 19 bytes (11 + 6 + 2):

```
01 | 00 06 | 6F 72 64 65 72 73 | 00 00 00 00 | 00 00 00 02 | 68 69
^    ^       ^                   ^             ^             ^
|    |       "orders"            partition=0   msg length=2  "hi"
|    topic length = 6
request type = PRODUCE
```

### Why the length prefixes matter
The broker reads the stream one field at a time:
1. read 1 byte → it's a PRODUCE request
2. read 2 bytes → topic is 6 bytes long → read exactly 6 bytes
3. read 4 bytes → partition
4. read 4 bytes → message is 2 bytes long → read exactly 2 bytes

Without the length fields, the broker couldn't tell where `"orders"` ends and the
partition number starts. Real Kafka works the same way: every string and byte
array on the wire has a length prefix.

---

## `ByteBuffer` and `flip()`

A `ByteBuffer` keeps a few pointers:

- **position**: where the next read or write happens
- **limit**: the point where reading or writing stops
- **capacity**: the total size

```
After allocate(19):     position=0,  limit=19
After all the put()s:   position=19, limit=19   ← the "cursor" is at the END
After flip():           position=0,  limit=19   ← ready to be READ from the start
```

Each `put()` moves `position` forward. Whoever uses the buffer next
(e.g. `socketChannel.write(buffer)`) reads from `position` to `limit`.

**`flip()` switches the buffer from writing mode to reading mode**:
`limit = position; position = 0;`

### What if you forget `flip()`?
`channel.write(buffer)` sees `position == limit` → 0 bytes remaining → sends **nothing**.
No exception. The broker just waits forever for a request that never comes.

### When the buffer is bigger than the data
If you `allocate(1024)` and write 19 bytes, `flip()` sets `limit = 19`, so the
unused 1005 bytes are never sent. In encodeProduceRequest the size is exact,
so only the position reset matters.

---

## Pitfall: characters vs bytes

`topic.length()` counts **characters**, not **bytes**. For ASCII they match;
for Cyrillic or other non-ASCII text, UTF-8 needs 2+ bytes per character:

```java
"orders".length()     // 6 chars → 6 bytes  ✓
"поръчки".length()    // 7 chars → 14 bytes in UTF-8  ✗
```

With such a topic the buffer is too small → `BufferOverflowException`.
Also `getBytes()` with no argument uses the platform default charset, so two
machines could disagree on the encoding.

Safer version:

```java
byte[] topicBytes = topic.getBytes(StandardCharsets.UTF_8);
ByteBuffer buffer = ByteBuffer.allocate(11 + topicBytes.length + message.length);
buffer.put(PRODUCE);
buffer.putShort((short) topicBytes.length);
buffer.put(topicBytes);
...
```

**Rule: compute lengths from the bytes you actually write, never from the String.**

---

## How real Kafka compares

Every real Kafka request starts with a **4-byte total size**, then a header:

- `api_key`: Produce = 0, Fetch = 1, Metadata = 3 (the real version of our `PRODUCE` byte)
- `api_version`
- `correlation_id`: lets the client match responses to requests
- `client_id`: this is your `client.id` config at work

The body follows the header. Errors like `RecordTooLargeException` belong to this layer.

---

## Exercise
Write `decodeProduceRequest(ByteBuffer)` yourself: read the fields back in the
same order (`get()`, `getShort()`, `get(byte[])`, `getInt()`, ...) and check that
encode → decode gives back `"orders"`, `0`, `"hi"`.

---

## `encodeFetchRequest`(String topic, int partition, long offset, int maxBytes)`

### Purpose
The opposite of produce. A **consumer** uses it to ask the broker:
*"Give me messages from topic X, partition Y, starting at offset Z, but no more than N bytes."*
Without this method there is no way to get data **out** of the broker.

### Wire layout

| Field        | Type  | Size (bytes) |
|--------------|-------|--------------|
| request type | byte  | 1            |
| topic length | short | 2            |
| topic        | bytes | N            |
| partition    | int   | 4            |
| offset       | long  | 8            |
| maxBytes     | int   | 4            |

Fixed part: `1 + 2 + 4 + 8 + 4 = 19`. No message payload, because the consumer is asking for data, not sending it.

### Example
`encodeFetchRequest("orders", 0, 5, 1024)` → 25 bytes (19 + 6):

```
02 | 00 06 | 6F 72 64 65 72 73 | 00 00 00 00 | 00 00 00 00 00 00 00 05 | 00 00 04 00
^    ^       "orders"            partition=0   offset=5                  maxBytes=1024
FETCH
```

Meaning: "In `orders` partition 0, start at message #5 and send me at most 1 KB."

### The fields
- **offset**: the position of a message inside a partition (0, 1, 2, ...). It's a `long`
  because a busy partition can hold more than 2^31 (~2.1 billion) messages over its lifetime.
- **maxBytes**: protects the consumer's memory and the network. Without it, asking for
  offset 0 on a huge partition could send gigabytes in one response.

### The consumer keeps the position, not the broker
The broker doesn't remember where each consumer stopped. The consumer sends the offset
**every time**. A typical consumer loop:

```
fetch(offset=0)  → gets messages 0..9  → process them → next offset = 10
fetch(offset=10) → gets messages 10..14 → process     → next offset = 15
fetch(offset=15) → nothing new yet     → wait, try again
```

Want to re-read everything? Send `offset=0` again. That's why Kafka can **replay** messages
and a traditional queue can't: messages aren't deleted when read.

### Real Kafka equivalent
- `consumer.poll()` sends **Fetch** requests (api_key = 1) under the hood.
- `max.partition.fetch.bytes` / `fetch.max.bytes` = our `maxBytes`.
- The offset you send = the consumer's position. After a restart it comes from the
  **committed offset** (`enable.auto.commit`, `commitSync()`). If there's no committed
  offset, `auto.offset.reset` (`earliest` / `latest`) decides where to start.
- `consumer.seek(partition, offset)` = just changing the offset you'll send next.

### What if a message is bigger than `maxBytes`?

**In the simple-kafka reference** (`Partition.readMessages`), the broker reads messages one by one and stops
*before* the one that would go over `maxBytes`:

```java
if (bytesRead + messageSize > maxBytes) break;
```

- Several small messages, the limit is reached in the middle → you get the ones that fit; the next fetch continues from there. Fine.
- **The first message at your offset is bigger than `maxBytes`** → the broker returns an **empty list**.
  The consumer only advances the offset when it gets messages (`currentOffset += messages.size()`),
  so it asks for the same offset again... and again... forever. The consumer is **stuck** and
  there is no error. It looks like "no new messages". This is called a **poison message**.

Example: `MAX_BYTES = 1 MB`, someone produces a 2 MB message at offset 42:
```
fetch(offset=42, maxBytes=1MB) → []   (message is 2 MB, doesn't fit)
fetch(offset=42, maxBytes=1MB) → []
fetch(offset=42, maxBytes=1MB) → []   ... stuck
```

**Real Kafka had exactly this problem.** Old consumers threw `RecordTooLargeException` and got stuck.
Since Kafka 0.10.1 (KIP-74) the broker **always returns at least the first record batch, even if it is
bigger than the fetch limit**, so the consumer can always make progress. `fetch.max.bytes` and
`max.partition.fetch.bytes` are soft limits, not hard ones.

Real Kafka also limits size on the **write** side, so huge messages are rejected before they are stored:
- producer `max.request.size` (default ~1 MB) → producer gets `RecordTooLargeException`
- broker `message.max.bytes` / topic `max.message.bytes` → broker rejects the batch

Lesson: **limit size where data comes in (produce), not only where it goes out (fetch).**

### Where does the consumer resume after a restart?

**In simple-kafka: it doesn't remember.** `SimpleKafkaConsumer.currentOffset` is just a field in memory.
After a restart it starts again from the `startOffset` passed to the constructor (e.g. 0) and
re-reads everything. There are no commits and no consumer groups.

**In real Kafka: committed offsets.**
- The consumer periodically **commits** "for group `G`, topic `T`, partition `P`, the next offset is `N`".
- Commits are stored by the broker in an internal topic, **`__consumer_offsets`**, keyed by `(group.id, topic, partition)`.
  One broker acts as the **group coordinator** for each group.
- After a restart, the consumer joins its group, gets its partitions, asks the coordinator
  "what's my committed offset?" and starts fetching from there.
- **No committed offset** (new group, or offsets expired) → `auto.offset.reset` decides: `earliest` (from the start) or `latest` (only new messages).
- The committed offset is the **next** offset to read (last processed + 1).
- The same thing happens on a **rebalance**: the consumer that takes over a partition starts from the committed offset.

**When you commit decides what a crash costs you:**

```
Commit AFTER processing (at-least-once):
  fetch 10..14 → process 10..14 → 💥 crash → (commit never happens)
  restart → committed = 10 → process 10..14 AGAIN → duplicates

Commit BEFORE processing (at-most-once):
  fetch 10..14 → commit 15 → process 10, 11 → 💥 crash
  restart → committed = 15 → 12..14 are never processed → lost
```

- **Auto-commit** (`enable.auto.commit=true`, every `auto.commit.interval.ms` = 5 s): inside `poll()` it commits
  what the *previous* `poll()` returned. A crash means up to ~5 s of messages are processed again. It's at-least-once,
  **unless** you hand messages to another thread: then a commit can happen before that thread is done → possible loss.
- **Manual commit** (`commitSync()` after processing): you control it; at-least-once.
- **At-least-once means duplicates will happen** → make processing **idempotent** (e.g. upsert by a unique ID).
- **Exactly-once**: Kafka transactions (consume → produce to Kafka), or store the offset in the same DB transaction as the result.

**Gotcha:** committed offsets expire if the group is inactive longer than `offsets.retention.minutes`
(default 7 days). A consumer that was stopped for longer starts from `auto.offset.reset` again.

### How to tell if a consumer is at-least-once or at-most-once

There is **no single setting**. It comes from the config **plus** where the code commits:

| `enable.auto.commit` | Code                                                | Result                                  |
|----------------------|-----------------------------------------------------|-----------------------------------------|
| `true`               | processes messages in the poll loop (same thread)   | at-least-once                           |
| `true`               | hands messages to another thread / async            | can **lose** messages (no guarantee)    |
| `false`              | `commitSync()` / `commitAsync()` **after** processing | at-least-once                         |
| `false`              | commits **before** processing                       | at-most-once                            |

Spring Kafka (`@KafkaListener`): auto-commit is off by default and the container commits for you
after the listener returns (`AckMode.BATCH`) → at-least-once. With `AckMode.MANUAL`, look for `ack.acknowledge()`.

Check at runtime:
```
kafka-consumer-groups.sh --bootstrap-server <host:9092> --describe --group <group.id>
```
Shows per partition `CURRENT-OFFSET` (committed), `LOG-END-OFFSET` (latest), and `LAG` (difference).

---

---

## What is a broker?

A **broker is one Kafka server**: a running program (a JVM process) on a machine, listening on a
TCP port (usually 9092). It:
1. **receives** messages from producers (Produce requests) and **writes them to disk**
2. **sends** stored messages to consumers (Fetch requests)
3. **answers questions** about the cluster (Metadata requests)

Producers and consumers never talk to each other. They only talk to brokers.
The broker is the "post office" in the middle.

```
Producer ──Produce──▶  BROKER  ──Fetch──▶ Consumer
                      (stores messages on disk)
```

Several brokers working together = a **Kafka cluster**. Partitions are spread across them, so
the load is shared and a copy survives if one broker dies.

On disk each partition is a folder of log files, e.g. `/var/lib/kafka/data/orders-0/00000000000000000000.log`
(config `log.dirs`).

In simple-kafka the broker is `SimpleKafkaBroker.java`: it opens a socket, reads the bytes produced by
our `encode...Request` methods, decodes them, and writes/reads messages in `Partition.java`.
`BrokerInfo` (id, host, port) is how one broker is described to clients.

# Stage 2 – Protocol Layer Notes

Source: https://buildthingsuseful.medium.com/stage-2-building-your-own-kafka-creating-the-core-protocol-layer-5838c1b2364c
Code: `src/main/java/com/simplekafka/broker/Protocol.java`
Broker concepts (leaders, replicas, failures): see `brokers.md`
Consumer concepts (commits, restarts, delivery guarantees): see `consumers.md`

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

## `encodeProduceRequest` (String topic, int partition, byte[] message)`

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

## `encodeFetchRequest` (String topic, int partition, long offset, int maxBytes)`

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

---

## `encodeMetadataRequest`

### Purpose
The client asks a broker: **"Tell me about the cluster."** Which brokers exist, which topics exist,
and for every partition, which broker is the **leader**. Without it the client would only know the
one broker address you configured, and wouldn't know where to send produce/fetch requests.

### Wire layout

| Field        | Type | Size (bytes) |
|--------------|------|--------------|
| request type | byte | 1            |

That's all: `01` would be produce, `03` is metadata. There are no parameters, because this simple
version always asks for **everything**. The whole request is one byte:

```
03
```

### Example
Cluster: 3 brokers, topic `orders` with 2 partitions, replicated on 2 brokers each.

Client config only knows `localhost:9091`. It sends `03` there and gets back (in plain words; the exact bytes are covered with `decodeMetadataResponse`):

```
brokers:
  1 → localhost:9091
  2 → localhost:9092
  3 → localhost:9093
topics:
  orders
    partition 0 → leader 1, replicas [1, 2]
    partition 1 → leader 2, replicas [2, 3]
```

Now `producer.send("orders", partition 1, ...)`:
1. look up `orders` / partition 1 → leader is broker 2
2. look up broker 2 → `localhost:9092`
3. open a connection to `localhost:9092` and send the produce request there

### When the client asks again (refresh)
The client **caches** the metadata and refreshes it when it's missing or wrong
(`SimpleKafkaClient.refreshMetadata()` in the reference):
- on startup
- when the topic isn't in the cache (e.g. it was just created)
- when the leader broker id isn't known

If a broker dies, another replica becomes leader. Old metadata now points to a dead broker,
so the client must refresh to find the new leader.

### What if you don't have it?
The client would have to hard-code which broker owns which partition. Add a broker, move a
partition, or lose a leader → every client breaks. Metadata makes the cluster **discoverable**:
clients need just one address to get in, and learn the rest.

### Real Kafka equivalent
- Metadata request is api_key = 3. Unlike ours, it takes a **list of topics** (only ask about what you use).
- `bootstrap.servers` = the address(es) used for the first metadata request. You don't need to list
  every broker, 2-3 is enough (more than one, in case the first is down).
- Clients refresh metadata at least every `metadata.max.age.ms` (default 5 min), and immediately on
  errors like `NOT_LEADER_OR_FOLLOWER` or `UNKNOWN_TOPIC_OR_PARTITION`.
- **Common gotcha:** the broker returns its **`advertised.listeners`** address, not the address you
  connected to. If that's set to an internal hostname (e.g. a Docker container name), the client
  connects to bootstrap fine, gets metadata, then fails to reach the leader. "Connects, then times out"
  is very often this.

---

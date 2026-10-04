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

## `encodeProduceRequest` (String topic, int partition, byte[] message)

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

## `encodeFetchRequest` (String topic, int partition, long offset, int maxBytes)

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

## `encodeCreateTopicRequest` (String topic, int numPartitions, short replicationFactor)`

### Purpose
The client asks a broker: **"Create topic X, split into N partitions, each stored on R brokers."**
Produce and fetch need a topic and partition to exist first, and this is how they get created.

### Wire layout

| Field             | Type  | Size (bytes) |
|-------------------|-------|--------------|
| request type      | byte  | 1            |
| topic length      | short | 2            |
| topic             | bytes | N            |
| numPartitions     | int   | 4            |
| replicationFactor | short | 2            |

Fixed part: `1 + 2 + 4 + 2 = 9`.

### Example
`encodeCreateTopicRequest("orders", 3, (short) 2)` → 15 bytes (9 + 6):

```
04 | 00 06 | 6F 72 64 65 72 73 | 00 00 00 03     | 00 02
^    ^       "orders"            numPartitions=3   replicationFactor=2
CREATE_TOPIC
```

Meaning: "Create `orders` with 3 partitions, and keep 2 copies of each partition."
With 3 brokers, the cluster might place them like this (leaders spread out, copies on different brokers):

```
            broker 1     broker 2     broker 3
orders-0    LEADER       follower
orders-1                 LEADER       follower
orders-2    follower                  LEADER
```

(See `brokers.md` for leaders and replicas.)

### The two numbers
- **numPartitions**: how many pieces the topic is split into.
  - More partitions = more parallelism: in a consumer group, **each partition is read by at most one
    consumer**, so 3 partitions = at most 3 consumers working in parallel. A 4th consumer would sit idle.
  - Order is guaranteed only **inside** one partition, not across the whole topic.
- **replicationFactor**: how many brokers keep a copy of each partition.
  - `1` = no copies; if that broker dies, the partition is unavailable (or lost if the disk is gone).
  - It can't be more than the number of brokers: you can't keep 3 copies on 2 brokers.
  - It's a `short` because it's always a small number (usually 3); an `int` would just waste 2 bytes.

### What if this method didn't exist?
Clients couldn't create topics. The only options would be to create them by hand on the broker, or to have
the broker create them automatically on the first produce, with default settings you didn't choose.

### Real Kafka equivalent
- **CreateTopics** request (api_key = 19). You use it through `AdminClient.createTopics(...)` or
  `kafka-topics.sh --create --topic orders --partitions 3 --replication-factor 2`.
- `replication-factor` > number of brokers → `InvalidReplicationFactorException`.
- **Auto-creation:** with `auto.create.topics.enable=true`, producing to a missing topic creates it with
  `num.partitions` (default 1) and `default.replication.factor` (default 1). A typo in a topic name then
  quietly creates a new topic with **1 partition and no copies**. Many production clusters turn it off.
- **Partitions can be increased later, never decreased.** Increasing them changes which partition a key
  goes to (`hash(key) % numPartitions`), so messages with the same key may land in a different partition
  than before, and per-key ordering breaks during the switch. Choose the count carefully up front.
- **Replication factor** can't be changed with a simple setting; it needs a partition reassignment
  (`kafka-reassign-partitions.sh`).

---

## `encodeReplicateRequest` (String topic, int partition, long offset, byte[] message)

### Purpose
This is **not** sent by a client. It's **broker-to-broker**: the leader of a partition sends it to
each follower, saying *"store this message in topic X, partition Y, at offset Z"*.
This is how followers get their copies (see `brokers.md`, leaders and replicas).

### Wire layout

| Field          | Type  | Size (bytes) |
|----------------|-------|--------------|
| request type   | byte  | 1            |
| topic length   | short | 2            |
| topic          | bytes | N            |
| partition      | int   | 4            |
| offset         | long  | 8            |
| message length | int   | 4            |
| message        | bytes | M            |

Fixed part: `1 + 2 + 4 + 8 + 4 = 19`.

It's the produce request **plus an offset**.

### Example
Broker 2 leads `orders-1`, broker 3 is the follower.

1. A producer sends "hi" to broker 2 (produce request, **no offset**).
2. Broker 2 appends it and assigns **offset 7**.
3. Broker 2 sends to broker 3: `encodeReplicateRequest("orders", 1, 7, "hi".getBytes())` → 27 bytes (19 + 6 + 2):

```
21 | 00 06 | 6F 72 64 65 72 73 | 00 00 00 01 | 00 00 00 00 00 00 00 07 | 00 00 00 02 | 68 69
^    ^       "orders"            partition=1   offset=7                  msg length=2  "hi"
REPLICATE
```

4. Broker 3 stores "hi" at offset 7 in its copy of `orders-1`.

### Why the offset is included here, but not in produce
The **leader decides offsets**. A producer can't choose one, so produce has no offset field.
A follower must not choose one either: its copy has to be **identical** to the leader's,
otherwise "offset 7" would mean different messages on different brokers. So the leader tells it the exact offset.

If a consumer reads `orders-1` offset 7 from broker 2 today, and broker 3 becomes the leader tomorrow,
offset 7 must still be "hi".

### What if this method didn't exist?
Followers would never get any data. `replicationFactor` would be just a number: if the leader dies,
the new leader has an empty partition, and the messages are lost.

### Push vs pull: simple-kafka vs real Kafka
- **simple-kafka = push.** The leader sends each message to the followers (this request).
- **Real Kafka = pull.** There is no "replicate" request. Followers send normal **Fetch** requests to
  the leader (with their broker id, so the leader knows it's a replica, not a consumer), exactly like a consumer.

Why Kafka pulls:
- each follower fetches at its own speed, and a slow follower doesn't slow down the leader
- followers fetch many messages in one request (batching) instead of one request per message
- the offset in the follower's fetch tells the leader how far that follower got, which is how the leader
  knows who is in the **ISR** and when an `acks=all` write is safely copied

### Bug in the GitHub reference
The reference allocates `17 + topic.length() + message.length`, but writes 19 fixed bytes.
Every call throws `BufferOverflowException`. It goes unnoticed because the reference broker never calls
this method: it builds the same bytes inline in `replicateToFollowers()`, with the same wrong size.
Worse: `BufferOverflowException` is a `RuntimeException`, not an `IOException`, so the `catch (IOException e)`
there doesn't catch it. It's thrown inside `executor.submit(...)`, and nobody reads the `Future`, so the
exception disappears **silently**: no log, no error, and the followers never get a copy.
Lesson: compute buffer sizes from the fields you write, and don't swallow exceptions in background tasks.

---

## `encodeTopicNotification` (String topic)

### Purpose
Another **broker-to-broker** message. After a topic is created, the **controller** (the broker in charge
of the cluster) tells the other brokers: *"topic X now exists, go load its details."*

### Wire layout

| Field        | Type  | Size (bytes) |
|--------------|-------|--------------|
| request type | byte  | 1            |
| topic length | short | 2            |
| topic        | bytes | N            |

Fixed part: `1 + 2 = 3`.

### Example
`encodeTopicNotification("orders")` → 9 bytes (3 + 6):

```
23 | 00 06 | 6F 72 64 65 72 73
^    ^       "orders"
TOPIC_NOTIFICATION
```

### Where it fits: creating a topic across the cluster
```
1. client ──create_topic("orders", 3, 2)──▶ controller (broker 1)
2. controller decides the placement:
      orders-0 → leader 1, replicas [1, 2]
      orders-1 → leader 2, replicas [2, 3]
      orders-2 → leader 3, replicas [3, 1]
   and saves it in ZooKeeper (the shared metadata store)
3. controller ──topic_notification("orders")──▶ broker 2
   controller ──topic_notification("orders")──▶ broker 3
4. brokers 2 and 3 read the "orders" details from ZooKeeper and create their partitions on disk
```

### Why only the name?
The full details (which partitions, leaders, replicas) are already in ZooKeeper. The notification is just
a **"wake up, something changed"** signal. Every broker reads the same data from one place, so there's
**one source of truth**, and the message stays tiny.

### What if this method didn't exist?
Only the controller would know about the new topic. Example: a producer sends to `orders-1`, metadata
says the leader is broker 2, but broker 2 never heard of `orders` → it answers "unknown topic".
The topic exists on paper but nobody except the controller can serve it, until the brokers restart and
reload everything from ZooKeeper.

### Real Kafka equivalent
- **Older Kafka (ZooKeeper mode):** same idea. The controller wrote to ZooKeeper and sent
  `LeaderAndIsr` / `UpdateMetadata` requests to the brokers.
- **Kafka 3.3+ (KRaft mode, ZooKeeper removed in 4.0):** no notifications. The controllers keep an
  internal metadata log (`__cluster_metadata`), and every broker **pulls** it continuously, like a follower
  fetching a partition. A new topic is just a new record in that log. Same pull design as replication.

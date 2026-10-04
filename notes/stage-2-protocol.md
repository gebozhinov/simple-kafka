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

| Field          | Type  | Size (bytes) |
|----------------|-------|--------------|
| request type   | byte  | 1            |
| topic length   | short | 2            |
| topic          | bytes | N (variable) |
| partition      | int   | 4            |
| message length | int   | 4            |
| message        | bytes | M (variable) |

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

Each `put()` moves `position` forward. Whoever uses the buffer next (e.g. `socketChannel.write(buffer)`) reads from
`position` to `limit`.

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
"поръчки".

length()    // 7 chars → 14 bytes in UTF-8  ✗
```

With such a topic the buffer is too small → `BufferOverflowException`.
Also `getBytes()` with no argument uses the platform default charset, so two
machines could disagree on the encoding.

Safer version:

```java
byte[] topicBytes = topic.getBytes(StandardCharsets.UTF_8);
ByteBuffer buffer = ByteBuffer.allocate(11 + topicBytes.length + message.length);
buffer.

put(PRODUCE);
buffer.

putShort((short) topicBytes.length);
        buffer.

put(topicBytes);
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

The broker doesn't remember where each consumer stopped. The consumer sends the offset **every time**. A typical
consumer loop:

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
- The offset you send = the consumer's position. After a restart it comes from the **committed offset**
  (`enable.auto.commit`, `commitSync()`). If there's no committed
  offset, `auto.offset.reset` (`earliest` / `latest`) decides where to start.
- `consumer.seek(partition, offset)` = just changing the offset you'll send next.

### What if a message is bigger than `maxBytes`?

**In the simple-kafka reference** (`Partition.readMessages`), the broker reads messages one by one and stops *before*
the one that would go over `maxBytes`:

```java
if(bytesRead +messageSize >maxBytes)break;
```

- Several small messages, the limit is reached in the middle → you get the ones that fit; the next fetch continues from
  there. Fine.
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

Client config only knows `localhost:9091`. It sends `03` there and gets back (in plain words; the exact bytes are
covered with `decodeMetadataResponse`):

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

The client **caches** the metadata and refreshes it when it's missing or wrong (`SimpleKafkaClient.refreshMetadata()` in
the reference):

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
- `bootstrap.servers` = the address (es) used for the first metadata request. You don't need to list
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
a **"wake up, something changed"** signal. Every broker reads the same data from one place, so there's **one source of
truth**, and the message stays tiny.

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

---

## `decodeProduceResponse` (ByteBuffer buffer)

### Purpose

The first **decode** method: the reverse of encoding. After the producer sends a produce request,
the broker answers with bytes. This method turns those bytes back into a Java object, `ProduceResult`,
that tells the producer **did it work, and at which offset was my message stored?**

```
producer ──encodeProduceRequest──▶ broker
producer ◀──── response bytes ──── broker
         decodeProduceResponse → ProduceResult(offset=7, error=null)
```

### The broker can answer in two shapes

The first byte says which shape follows, so the method **must read it first**.

**1. A produce response** (built in the reference `SimpleKafkaBroker`):

| Field         | Type | Size | Meaning                               |
|---------------|------|------|---------------------------------------|
| response type | byte | 1    | `PRODUCE_RESPONSE` (0x11)             |
| offset        | long | 8    | where the message was stored          |
| status        | byte | 1    | `0` = success, anything else = failed |

**2. An error response** (`sendErrorResponse`), e.g. topic not found:

| Field         | Type  | Size | Meaning                 |
|---------------|-------|------|-------------------------|
| response type | byte  | 1    | `ERROR_RESPONSE` (0x1F) |
| error length  | short | 2    | how many bytes of text  |
| error message | bytes | N    | the text                |

### Line by line

```java
byte responseType = buffer.get();             // read 1 byte, cursor moves to byte 1
if(responseType !=PRODUCE_RESPONSE){       // not the normal answer?
        if(responseType ==ERROR_RESPONSE){     //   is it a broker error with a message?
short errorLength = buffer.getShort();        // 2 bytes: how long is the text
byte[] errorBytes = new byte[errorLength];    // make room for exactly that many bytes
        buffer.

get(errorBytes);                       // copy them out of the buffer

String error = new String(errorBytes);        // bytes → text
        return new

ProduceResult(-1,error);          // -1 = "no offset", there's no message stored
    }
            return new

ProduceResult(-1,"Invalid response type");  // a byte we don't recognize at all
}
long offset = buffer.getLong();               // 8 bytes: the offset
byte status = buffer.get();                   // 1 byte: 0 = ok
return new

ProduceResult(offset, status ==0?null:"Produce failed");
```

It's the same length-prefix idea as encoding, in reverse: read the length, then read exactly that many bytes.

### Examples

**Success:** message stored at offset 7

```
11 | 00 00 00 00 00 00 00 07 | 00
^    offset=7                  status=0 (ok)
PRODUCE_RESPONSE
→ ProduceResult(offset=7, error=null)   isSuccess() = true
```

**Broker error:** the topic doesn't exist

```
1F | 00 0F | 54 6F 70 69 63 20 6E 6F 74 20 66 6F 75 6E 64
^    len=15  "Topic not found"
ERROR_RESPONSE
→ ProduceResult(offset=-1, error="Topic not found")   isSuccess() = false
```

**Write failed:** the broker understood the request but couldn't store it

```
11 | FF FF FF FF FF FF FF FF | 01
     offset=-1                 status=1
→ ProduceResult(offset=-1, error="Produce failed")
```

**Unknown type**, e.g. `12 ...` (a FETCH_RESPONSE) → `ProduceResult(-1, "Invalid response type")`.
This usually means the client and broker are **out of sync**: the client is reading from the wrong
position in the stream, so the rest of the bytes are garbage too. Real clients close the connection then.

### `ProduceResult` conventions

- `error == null` means success, that's what `isSuccess()` checks.
- `offset == -1` means "no offset" (offsets start at 0, so -1 can't be a real one).

### Why the order of `get()` calls matters

Each `get...()` moves the cursor forward (same cursor as in `flip()`). Read the fields in a different order
than the broker wrote them, and you get wrong values with **no error**: e.g. reading `status` before
`offset` would take the first byte of the offset as the status.

### Pitfall

`new String(errorBytes)` uses the platform default charset. Use `new String(errorBytes, StandardCharsets.UTF_8)`,
same reason as `getBytes(StandardCharsets.UTF_8)` when encoding: both sides must agree on the charset.

### What if this method didn't exist?

The producer would send messages and never know if they were stored. No offset, no error: a topic-not-found
or a disk-full broker would look exactly like success.

### Real Kafka equivalent

- The Produce response has, per partition, an **error code** (a number, not text, e.g. `3 = UNKNOWN_TOPIC_OR_PARTITION`)
  and the **base offset** of the batch.
- This is what you get in the producer callback: `RecordMetadata.offset()` on success, or the exception.
  ```java
  producer.send(record, (metadata, exception) -> {
      if (exception == null) System.out.println("stored at offset " + metadata.offset());
      else                   System.out.println("failed: " + exception);
  });
  ```
- With `acks=0` there is **no response at all**, so the offset is unknown (`-1`) and errors are never reported.

---

## `decodeFetchResponse` (ByteBuffer buffer)

### Purpose

The consumer sent `encodeFetchRequest("orders", 0, offset=5, maxBytes=1024)`. The broker answers with
the messages it found. This method turns those bytes into a `FetchResult`: **the list of messages**,
or an **error**.

```
consumer ──encodeFetchRequest──▶ broker (reads messages from the partition log)
consumer ◀─── response bytes ─── broker
         decodeFetchResponse → FetchResult(messages=[...], error=null)
```

### The broker can answer in two shapes (first byte decides)

**1. A fetch response:**

```
[FETCH_RESPONSE:1][messageCount:4]
    then messageCount times:  [offset:8][messageSize:4][message:M]
```

| Field          | Type  | Size | Meaning                               |
|----------------|-------|------|---------------------------------------|
| response type  | byte  | 1    | `FETCH_RESPONSE` (0x12)               |
| message count  | int   | 4    | how many messages follow (can be 0)   |
| *per message:* |       |      |                                       |
| offset         | long  | 8    | the message's offset in the partition |
| message size   | int   | 4    | how many bytes the message has        |
| message        | bytes | M    | the message itself                    |

This is new compared to produce: a **repeated group**. The count says how many times to repeat
the `[offset][size][message]` block, and each block has its own length prefix.

**2. An error response:** `[ERROR_RESPONSE:1][errorLen:2][error:N]`, same as in produce (e.g. "Topic does not exist",
"Partition does not exist").

### Line by line

```java
byte responseType = buffer.get();                 // 1 byte: which shape?
if(responseType !=FETCH_RESPONSE){
        if(responseType ==ERROR_RESPONSE){         // broker error with a text message
short errorLength = buffer.getShort();
byte[] errorBytes = new byte[errorLength];
        buffer.

get(errorBytes);

String error = new String(errorBytes, StandardCharsets.UTF_8);
        return new

FetchResult(new byte[0][], error);    // no messages + the error
    }
            return new

FetchResult(new byte[0][], "Invalid response type");
}

int messageCount = buffer.getInt();               // 4 bytes: how many messages follow
byte[][] messages = new byte[messageCount][];     // an array of messages; each message is a byte[]
// (sizes unknown yet, so the inner arrays are null for now)
for(
int i = 0;
i<messageCount;i++){
long offset = buffer.getLong();               // 8 bytes: the offset (read, then thrown away)
int messageSize = buffer.getInt();            // 4 bytes: size of this message
messages[i]=new byte[messageSize];          // room for exactly this message
        buffer.

get(messages[i]);                      // copy its bytes out
}
        return new

FetchResult(messages, null);
```

**`byte[][]`** = "array of byte arrays". One message is a `byte[]`, so many messages are a `byte[][]`.
`new byte[3][]` creates 3 empty slots; each slot gets its own `new byte[messageSize]` in the loop,
because every message can have a different size.

**`new byte[0][]`** = an empty list of messages, so the caller can loop over `messages` without
checking for `null`.

**Why the offset is read even though it's not used:** the bytes are there in the stream. If you skip
`getLong()`, the cursor stays 8 bytes behind and `getInt()` reads part of the offset as the message size.
Everything after that is garbage. You must read (or skip with `buffer.position(buffer.position() + 8)`)
every field the broker wrote.

### Examples

**Two messages** from `encodeFetchRequest("orders", 0, 5, 1024)`:

```
12 | 00 00 00 02 |
     count=2
     00 00 00 00 00 00 00 05 | 00 00 00 02 | 68 69        offset=5, size=2, "hi"
     00 00 00 00 00 00 00 06 | 00 00 00 03 | 79 6F 75     offset=6, size=3, "you"
→ FetchResult(messages=["hi", "you"], error=null)
```

The consumer's next fetch uses offset 5 + 2 = **7**.

**No new messages** (offset is at the end of the log):

```
12 | 00 00 00 00
     count=0
→ FetchResult(messages=[], error=null)
```

This is **not an error**. It means "you're caught up, ask again later". (It's also what the consumer gets
when the next message is bigger than `maxBytes`, see the poison message section above.)

**Error:**

```
1F | 00 14 | "Topic does not exist"   (20 bytes = 0x14)
→ FetchResult(messages=[], error="Topic does not exist")
```

### Throwing away the offset: a design weakness

Because the offsets are discarded, the consumer must **assume** they are consecutive: "I asked for 5 and
got 2 messages, so they were 5 and 6, next is 7". That's how the reference consumer works
(`currentOffset += messages.size()`).

In real Kafka that assumption is **wrong**: offsets can have gaps (log compaction removes old records,
transactions add invisible marker records). That's why every Kafka `ConsumerRecord` carries its own
`offset()`, and the next position is `lastRecord.offset() + 1`, not `start + count`.
Keeping the offset in the result is the safer design.

### Pitfalls

- **Trusting the numbers:** `messageCount` and `messageSize` come from the network. Corrupted bytes could say
  "2 billion messages" → `new byte[2_000_000_000][]` → `OutOfMemoryError`, or a negative size →
  `NegativeArraySizeException`. Real Kafka validates sizes and checks a CRC (checksum) on each batch.
- **Incomplete data:** a big response can arrive in several TCP pieces. If the buffer holds only part of it,
  `get()` runs past the end → `BufferUnderflowException`. The code that reads from the socket must collect
  the whole response before decoding.

### What if this method didn't exist?

The consumer could ask for messages but couldn't read the answer. Consuming would be impossible.

### Real Kafka equivalent

- Fetch response (api_key 1): per partition an error code, the **high watermark** (the last offset
  that's safely copied to all ISR replicas, consumers can't read past it), and **record batches**.
- What you see in code: `consumer.poll()` returns `ConsumerRecords`, each `ConsumerRecord` has
  `offset()`, `partition()`, `key()`, `value()`, `timestamp()`, `headers()`. Ours has only the value.
- An empty `poll()` result is normal, like our `count=0`.

---

## `decodeMetadataResponse` (ByteBuffer buffer)

### Purpose

The answer to `encodeMetadataRequest()`. The client asked "tell me about the cluster", and this method
turns the broker's answer into a `MetadataResult`: **all brokers** (where they are) and **all topics**
(their partitions, and which broker leads each one). The client uses it to know where to send produce
and fetch requests.

```
client ──encodeMetadataRequest (1 byte: 03)──▶ broker
client ◀────────── answer bytes ────────────── broker (handleMetadataRequest)
       decodeMetadataResponse → MetadataResult(brokers, topics)
```

### Layout: two lists, the second one nested

```
[METADATA_RESPONSE:1]

[brokerCount:4]
    brokerCount times:   [brokerId:4][hostLen:2][host:N][port:4]

[topicCount:4]
    topicCount times:    [topicLen:2][topic:N][partitionCount:4]
        partitionCount times:  [partitionId:4][leaderId:4][followerCount:4]
            followerCount times:   [followerId:4]
```

It's a **tree**: the cluster has topics, a topic has partitions, a partition has followers.
That's why the code has a loop inside a loop inside a loop. Each level starts with a **count** that says
how many items follow, the same repeated-group idea as in `decodeFetchResponse`.

Error shape is the same as before: `[ERROR_RESPONSE:1][errorLen:2][error:N]`.

### Line by line

```java
byte responseType = buffer.get();                       // which shape?
if(responseType !=METADATA_RESPONSE){...}          // error → empty lists + error text

// ---- part 1: brokers ----
int brokerCount = buffer.getInt();                      // how many brokers follow
List<BrokerInfo> brokers = new ArrayList<>();
for(
int i = 0;
i<brokerCount;i++){
int brokerId = buffer.getInt();                     // 4 bytes: id
short hostLength = buffer.getShort();               // 2 bytes: length of host name
byte[] hostBytes = new byte[hostLength];
    buffer.

get(hostBytes);                              // N bytes: host name

String host = new String(hostBytes, StandardCharsets.UTF_8);
int port = buffer.getInt();                         // 4 bytes: port
    brokers.

add(new BrokerInfo(brokerId, host, port));  // ← this is where BrokerInfo is used
        }

// ---- part 2: topics ----
int topicCount = buffer.getInt();                       // how many topics follow
List<TopicMetadata> topics = new ArrayList<>();
for(
int i = 0;
i<topicCount;i++){                  // LEVEL 1: each topic
short topicLength = buffer.getShort();
byte[] topicBytes = new byte[topicLength];
    buffer.

get(topicBytes);

String topicName = new String(topicBytes, StandardCharsets.UTF_8);

int partitionCount = buffer.getInt();               // how many partitions this topic has
List<PartitionMetadata> partitions = new ArrayList<>();
    for(
int j = 0;
j<partitionCount;j++){          // LEVEL 2: each partition of this topic
int partitionId = buffer.getInt();
int leaderId = buffer.getInt();                 // which broker leads it

int replicas = buffer.getInt();                 // how many followers
List<Integer> replicaIds = new ArrayList<>();
        for(
int k = 0;
k<replicas;k++){            // LEVEL 3: each follower id
        replicaIds.

add(buffer.getInt());
        }
        partitions.

add(new PartitionMetadata(partitionId, leaderId, replicaIds));
        }
        topics.

add(new TopicMetadata(topicName, partitions));
        }
        return new

MetadataResult(brokers, topics, null);
```

Note: `leaderId` is just a number. To connect, the client looks it up in the **brokers** list from part 1
to get the host and port. That's why brokers come first and partitions refer to them by id.

### Example

Cluster: 2 brokers, topic `orders` with 2 partitions, replication factor 2.

```
13                                              METADATA_RESPONSE
00 00 00 02                                     brokerCount = 2
   00 00 00 01 | 00 09 | "localhost" | 00 00 23 83     broker 1, localhost:9091
   00 00 00 02 | 00 09 | "localhost" | 00 00 23 84     broker 2, localhost:9092
00 00 00 01                                     topicCount = 1
   00 06 | "orders" | 00 00 00 02                      topic "orders", 2 partitions
      00 00 00 00 | 00 00 00 01 | 00 00 00 01 | 00 00 00 02     partition 0, leader 1, 1 follower: [2]
      00 00 00 01 | 00 00 00 02 | 00 00 00 01 | 00 00 00 01     partition 1, leader 2, 1 follower: [1]
```

(9091 = 0x2383, 9092 = 0x2384. Total 91 bytes.)

Result:

```
MetadataResult(
  brokers = [BrokerInfo(1, localhost, 9091), BrokerInfo(2, localhost, 9092)],
  topics  = [TopicMetadata("orders", [
                PartitionMetadata(0, leader=1, replicas=[2]),
                PartitionMetadata(1, leader=2, replicas=[1])
            ])],
  error   = null)
```

Using it: "send to `orders` partition 1" → partition 1 has leader 2 → broker 2 is `localhost:9092` → connect there.

### Naming trap: "replicas" here means followers only

The reference broker writes `partition.getFollowers()`, and when a topic is created the followers list **doesn't include
the leader**. So for replication factor 2 the list has 1 id, not 2.
In real Kafka, "replicas" **includes** the leader (`replicas [1, 2]`). Calling the field `followers`
would be more accurate here.

### Records with `List` vs records with arrays

In `FetchResult` the record held a `byte[][]`, and `equals()` compared array references.
Here the records hold `List`s, and `List.equals()` compares **contents**, so two `MetadataResult`s with the
same brokers and topics are equal, and `toString()` prints readable values. Lists are the better fit for records.

### Pitfalls

- Same as the other decoders: `new String(...)` should use `StandardCharsets.UTF_8` (3 places here).
- The broker computes the response size with `host.length()` and `topic.length()` (characters, not bytes),
  so a non-ASCII host or topic name breaks the response on the broker side. Remember it for the broker stage.
- If the response is incomplete (half arrived over TCP), the nested loops hit `BufferUnderflowException`.

### What if this method didn't exist?

The client could ask about the cluster but couldn't read the answer. It would never learn which broker leads
which partition, so it couldn't route produce or fetch requests anywhere but the one bootstrap broker.

### Real Kafka equivalent

The real Metadata response contains more per partition and per topic:

- **ISR** (in-sync replicas) and offline replicas, not just the replica list
- an **error code** per topic/partition, e.g. `LEADER_NOT_AVAILABLE` during leader election
- `leader = -1` when a partition has no leader at the moment (that's `OfflinePartitionsCount` > 0)
- the **controller id** and the **cluster id**

`kafka-topics.sh --describe` shows the same information:

```
Topic: orders  Partition: 0  Leader: 1  Replicas: 1,2  Isr: 1,2
Topic: orders  Partition: 1  Leader: 2  Replicas: 2,1  Isr: 2,1
```

# The Kafka Protocol (how real Kafka works)

General concepts, described the way **real Kafka** works. simple-kafka is mentioned only as a comparison.
Method explanations of our `Protocol` class live in `stage-2-protocol.md`.
Quiz: `quiz-4-cluster-internals.md`

Contents:
1. What the protocol is · 2. Who talks to whom · 3. Framing · 4. Request and response headers ·
5. API keys · 6. Versions and the ApiVersions handshake · 7. Data types · 8. A request byte by byte ·
9. The main requests, field by field · 10. Correlation ids and pipelining · 11. Errors ·
12. Fetch sessions · 13. Security: TLS and SASL · 14. Quotas and throttling · 15. What the article says ·
16. simple-kafka vs real Kafka · 17. Try it

---

## 1. What the protocol is

Kafka has its **own binary protocol over TCP**. It doesn't use HTTP or gRPC. Every client (Java, librdkafka, Go, …),
every broker and every controller speaks it. The rules are simple:

- **The client always starts.** It sends a request, and the broker sends exactly one response (except Produce
  with `acks=0`, which gets no response at all).
- **Long-lived connections.** One TCP connection per broker, kept open and reused for thousands of requests.
- **Every message is size-prefixed**, so the reader knows where it ends.
- **Every request type has versions.** Old and new clients and brokers can work together.

The protocol is documented at kafka.apache.org/protocol. Inside Kafka, each request is defined in a JSON file
(`ProduceRequest.json`, `FetchRequest.json`, …), and the Java classes that read and write it are **generated** from
those files.

---

## 2. Who talks to whom

```
                      Metadata (any broker)
Producer ───────────▶ Produce ───────────▶ leader of the partition
                      InitProducerId, AddPartitionsToTxn, EndTxn ─▶ transaction coordinator

Consumer ───────────▶ Fetch, ListOffsets ─▶ leader (or nearest follower)
                      FindCoordinator ─────▶ any broker
                      JoinGroup, SyncGroup, Heartbeat, OffsetCommit, OffsetFetch ─▶ group coordinator

Admin   ────────────▶ CreateTopics, DeleteTopics, DescribeConfigs, … ─▶ any broker (forwarded to the controller)

Follower broker ────▶ Fetch (with its replica id) ─▶ leader broker              (replication)
Broker ─────────────▶ BrokerRegistration, BrokerHeartbeat, AlterPartition ─▶ active controller
Controller ─────────▶ Vote, BeginQuorumEpoch, Fetch (__cluster_metadata) ─▶ other controllers   (Raft)
Broker ─────────────▶ Fetch (__cluster_metadata) ─▶ active controller          (metadata updates)
```

**Replication uses the same Fetch request as consumers.** A follower is just a special consumer of the leader.

---

## 3. Framing

Every request and every response on the wire:

```
┌────────────────┬──────────────────────────────────────────┐
│ size: int32    │ header + body: exactly `size` bytes      │
└────────────────┴──────────────────────────────────────────┘
```

The reader reads 4 bytes, then reads exactly `size` more bytes. Then it has one complete message, even if TCP delivered
it in 10 pieces or together with the next message. TCP is a **stream of bytes**, not a stream of messages. Without the
size you couldn't tell where one request ends and the next begins.

`socket.request.max.bytes` (default 100 MB) is the biggest request a broker accepts. A bigger size field closes the
connection. That also protects the broker from garbage, like an HTTP client connecting to port 9092 by mistake: "GET " read as
an int32 is 1,195,725,856, so the connection is closed instead of allocating 1 GB.

---

## 4. Request and response headers

```
Request header (v2)                          Response header (v1)
  request_api_key      int16   which API       correlation_id   int32   copied from the request
  request_api_version  int16   which version   _tagged_fields           (v1 only)
  correlation_id       int32   chosen by client
  client_id            nullable string (int16 length)  ← your client.id config
  _tagged_fields                (v2 only)
```

- **api_key + api_version** tell the broker how to read the body.
- **correlation_id** is a number the client picks, usually a counter. The response carries the same number, so the
  client knows which request it answers (see section 10).
- **client_id** appears in broker logs, metrics and quotas. Set `client.id` to something meaningful
  (`orders-service-producer`), because it's how you find a client on the broker side.
- The response header has **no api key**. The client knows what it asked for from the correlation id.

---

## 5. API keys

About 80 APIs. The ones that matter most:

| Key | API | Used by | Purpose |
|---|---|---|---|
| 0 | **Produce** | producer | write record batches |
| 1 | **Fetch** | consumer, follower, broker | read record batches (also replication and metadata) |
| 2 | ListOffsets | consumer | earliest / latest / offset for a timestamp |
| 3 | **Metadata** | all clients | brokers, topics, partitions, leaders |
| 8 | OffsetCommit | consumer | save the group's position |
| 9 | OffsetFetch | consumer | read the group's position |
| 10 | FindCoordinator | consumer, producer | which broker is my group / transaction coordinator? |
| 11 | JoinGroup | consumer | join a group (classic protocol) |
| 12 | Heartbeat | consumer | "I'm alive" (classic protocol) |
| 13 | LeaveGroup | consumer | leave the group on close |
| 14 | SyncGroup | consumer | get the assignment (classic protocol) |
| 17 / 36 | SaslHandshake / SaslAuthenticate | all | log in |
| 18 | **ApiVersions** | all | which versions do you support? |
| 19 / 20 | CreateTopics / DeleteTopics | admin | |
| 22 | InitProducerId | producer | get a producer id (idempotence / transactions) |
| 23 | OffsetForLeaderEpoch | follower, consumer | truncation after leader change |
| 24–26 | AddPartitionsToTxn, AddOffsetsToTxn, EndTxn | transactional producer | transactions |
| 32 / 44 | DescribeConfigs / IncrementalAlterConfigs | admin | configs |
| 52–55 | Vote, BeginQuorumEpoch, EndQuorumEpoch, DescribeQuorum | controllers | KRaft (Raft) |
| 56 | AlterPartition | leader broker → controller | ISR change |
| 62 / 63 | BrokerRegistration / BrokerHeartbeat | broker → controller | KRaft membership |
| 68 | ConsumerGroupHeartbeat | consumer | new consumer group protocol (KIP-848) |

ZooKeeper-era controller requests (`LeaderAndIsr` = 4, `StopReplica` = 5, `UpdateMetadata` = 6, `ControlledShutdown` = 7)
no longer exist in Kafka 4.0. In KRaft, brokers learn the same information by reading the metadata log (see `kraft.md`).

---

## 6. Versions and the ApiVersions handshake

Every API has versions: Produce v0 … v12, Fetch v0 … v17, and so on. A new version adds fields or changes behaviour,
for example Fetch v13 replaced topic names with topic ids.

**Each side supports a range**, and they use the **highest version both support**:

```
1. client connects to the broker
2. client → ApiVersions request (always readable by every broker)
3. broker → for every API key: min version, max version
      Produce  3..12   Fetch  4..17   Metadata 0..13   ...
4. client: "I support Produce up to 11" → uses Produce v11 on this connection
```

This is why a Kafka 2.8 client can talk to a Kafka 3.9 broker, and the other way around. Each side just picks an older
version. It only breaks if the ranges **don't overlap**. Kafka 4.0 removed many very old versions (KIP-896), so clients
older than about Kafka 2.1 can't connect to a 4.0 broker. They get `UNSUPPORTED_VERSION`.

simple-kafka has **no version field**. Adding one field to the produce request would break every older client.

---

## 7. Data types

| Type | Bytes | Notes |
|---|---|---|
| `int8`, `int16`, `int32`, `int64` | 1, 2, 4, 8 | **big-endian** (network order), like Java's `ByteBuffer` default |
| `uint32` | 4 | only for the CRC |
| `varint`, `varlong` | 1–5, 1–10 | variable length, **zig-zag** encoded: small numbers (also small negatives) take 1 byte |
| `unsigned_varint` | 1–5 | used for lengths in "compact" types |
| `string` | int16 length + UTF-8 bytes | |
| `nullable_string` | same, length -1 = null | |
| `compact_string` | unsigned_varint (length **+ 1**) + bytes | 0 = null, 1 = "", 6 = 5 bytes |
| `array` | int32 count + elements | -1 = null |
| `compact_array` | unsigned_varint (count + 1) + elements | |
| `bytes` / `records` | length + raw bytes | `records` = record batches (see `partition.md`) |
| `uuid` | 16 | topic ids |
| tagged fields | count + (tag, size, data)… | optional fields; old readers skip unknown tags |

**Flexible versions** (KIP-482, from Kafka 2.4): newer versions of each API use the compact types and have **tagged
fields** at the end of every structure. A tagged field can be added **without a new version**: old readers see an
unknown tag, read its size, and skip it. It's like adding a new optional JSON field, but in binary.

### Varint example (zig-zag)
```
value   zig-zag   bytes
  0        0      00
 -1        1      01
  1        2      02
 63      126      7E
 64      128      80 01        (7 bits per byte, high bit = "more bytes follow")
```

---

## 8. A request byte by byte

**ApiVersions v0**, client id `"cli"`, correlation id 1 (v0 uses request header v1, without tagged fields):

```
00 00 00 0D        size = 13 bytes follow
00 12              api_key = 18 (ApiVersions)
00 00              api_version = 0
00 00 00 01        correlation_id = 1
00 03 63 6C 69     client_id = "cli" (int16 length 3 + bytes)
                   (ApiVersions v0 has an empty body)
```

The response (shape):
```
00 00 01 9A        size
00 00 00 01        correlation_id = 1      ← matches the request
00 00              error_code = 0 (NONE)
00 00 00 4B        api_keys: array of 75 entries
00 00 00 03 00 0C    key 0 (Produce)  min 3  max 12
00 01 00 04 00 11    key 1 (Fetch)    min 4  max 17
...
```

Compare it with simple-kafka's produce request: `01 | 00 06 "orders" | 00 00 00 00 | 00 00 00 02 "hi"`.
Same ideas (a type, length-prefixed strings, big-endian ints), but there's no overall size, version or correlation id.

---

## 9. The main requests, field by field

Shown as trees (newer versions; `[]` = array). You don't need to memorise them. Look at **what information travels**.

### Produce
```
ProduceRequest
  transactional_id   nullable string      null unless transactions are used
  acks               int16                0, 1 or -1 (= all)
  timeout_ms         int32                how long the leader may wait for replication (acks=all)
  topic_data []
    name             string
    partition_data []
      index          int32                partition
      records        records              one or more record batches

ProduceResponse
  responses []
    name
    partition_responses []
      index, error_code
      base_offset        int64            offset given to the first record of the batch
      log_append_time_ms int64            -1 unless LogAppendTime
      log_start_offset   int64
      record_errors [], error_message     which record of the batch was invalid
  throttle_time_ms       int32            quota (section 14)
```
- **One request carries batches for many partitions**, as long as they all have the same leader. The producer
  groups its batches by broker.
- The response gives the **base offset** of each batch. Records in the batch are base + 0, + 1, ….

### Fetch
```
FetchRequest
  replica_state     replica_id: -1 for consumers, the broker id for followers
  max_wait_ms       int32                fetch.max.wait.ms   (long poll)
  min_bytes         int32                fetch.min.bytes     (wait until at least this much)
  max_bytes         int32                fetch.max.bytes     (whole response)
  isolation_level   int8                 0 read_uncommitted, 1 read_committed
  session_id, session_epoch               fetch sessions (section 12)
  topics []
    topic_id        uuid
    partitions []
      partition, current_leader_epoch     ← fencing: an old leader rejects it
      fetch_offset  int64                 the position
      last_fetched_epoch                  for follower truncation
      log_start_offset                    (followers only)
      partition_max_bytes                 max.partition.fetch.bytes
  forgotten_topics_data []                (fetch sessions)
  rack_id                                 client.rack, for follower fetching

FetchResponse
  throttle_time_ms, error_code, session_id
  responses []
    topic_id
    partitions []
      partition_index, error_code
      high_watermark, last_stable_offset, log_start_offset
      aborted_transactions []             read_committed consumers drop these
      preferred_read_replica              "fetch from broker 3 next time" (KIP-392)
      records                             whole record batches, straight from the file (zero-copy)
```
The response carries the **HW**. That's how followers learn it, and how consumers compute lag without extra requests.

### Metadata
```
MetadataRequest
  topics []                 null = all topics, [] = none (brokers only)
  allow_auto_topic_creation

MetadataResponse
  brokers []      node_id, host, port, rack        ← from advertised.listeners
  cluster_id
  controller_id   (in KRaft: a random broker, because clients can't reach controllers)
  topics []
    error_code, name, topic_id, is_internal
    partitions []
      error_code, partition_index
      leader_id, leader_epoch
      replica_nodes []      ALL replicas (leader included)
      isr_nodes []
      offline_replicas []
```
The client caches this and refreshes it every `metadata.max.age.ms` (5 min) **or** when it gets an error like
`NOT_LEADER_OR_FOLLOWER`.

---

## 10. Correlation ids and pipelining

A client doesn't wait for one response before sending the next request on the same connection:

```
client → broker:  Produce corr=7   Produce corr=8   Produce corr=9
broker → client:  resp corr=7      resp corr=8      resp corr=9
```

- `max.in.flight.requests.per.connection` (default 5) = how many requests may be waiting at the same time.
- The broker processes **one request at a time per connection** and answers **in order**. It mutes the
  connection while a request is being processed, which keeps order without any locks.
- The correlation id is how the client matches each response to its waiting request (and future/callback).

Without pipelining, every request would cost a full network round trip, and throughput would drop to
`1 / RTT` requests per second per connection.

---

## 11. Errors

Errors are **int16 codes**, usually **per partition**. One Produce request can succeed for partition 0 and fail for
partition 1. The client library maps each code to an exception and knows whether it's **retriable**.

| Code | Name | Retriable | Typical cause |
|---|---|---|---|
| 0 | NONE | | |
| 1 | OFFSET_OUT_OF_RANGE | | offset deleted by retention → `auto.offset.reset` |
| 2 | CORRUPT_MESSAGE | ✔ | CRC check failed |
| 3 | UNKNOWN_TOPIC_OR_PARTITION | ✔ | topic not created yet, or stale metadata |
| 6 | NOT_LEADER_OR_FOLLOWER | ✔ | leader moved → refresh metadata, retry |
| 7 | REQUEST_TIMED_OUT | ✔ | `acks=all` replication took longer than `timeout_ms` |
| 10 | MESSAGE_TOO_LARGE | | batch > `max.message.bytes` |
| 15 / 16 | COORDINATOR_NOT_AVAILABLE / NOT_COORDINATOR | ✔ | coordinator moving → FindCoordinator again |
| 19 | NOT_ENOUGH_REPLICAS | ✔ | ISR < `min.insync.replicas` (nothing written) |
| 20 | NOT_ENOUGH_REPLICAS_AFTER_APPEND | ✔ | written, but the ISR shrank before replication |
| 22 / 25 | ILLEGAL_GENERATION / UNKNOWN_MEMBER_ID | | consumer was kicked out of the group (rebalance) |
| 27 | REBALANCE_IN_PROGRESS | | commit during a rebalance |
| 29 | TOPIC_AUTHORIZATION_FAILED | | ACLs |
| 35 | UNSUPPORTED_VERSION | | no common version (section 6) |
| 36 | TOPIC_ALREADY_EXISTS | | |
| 45 / 46 | OUT_OF_ORDER_SEQUENCE_NUMBER / DUPLICATE_SEQUENCE_NUMBER | | idempotent producer checks |
| 74 / 75 | FENCED_LEADER_EPOCH / UNKNOWN_LEADER_EPOCH | ✔ | client has a newer/older leader epoch than the broker |

When you see `NotLeaderOrFollowerException` in your app logs during a broker restart, that's normal. The client refreshes
metadata and retries (`retries`, `delivery.timeout.ms` = 2 min for producers).

simple-kafka sends an error as `ERROR_RESPONSE + text`. The client can show it, but it can't decide automatically
whether to retry.

---

## 12. Fetch sessions (KIP-227)

A consumer (or follower) that reads 1000 partitions sends a Fetch every few hundred ms. Listing all 1000 partitions
in every request would waste bandwidth and CPU.

With a **fetch session**, the first request lists everything and the broker returns a `session_id`. Later requests
list only the **changes** ("partition 7: new offset"), with `forgotten_topics_data` for removed partitions, and the
response includes only partitions **that have new data**.

---

## 13. Security: TLS and SASL

Configured per **listener** (see `brokers.md`):

| `security.protocol` | Encryption | Login |
|---|---|---|
| PLAINTEXT | no | no |
| SSL | TLS | optional client certificate (mTLS) |
| SASL_PLAINTEXT | no | SASL |
| SASL_SSL | TLS | SASL (the usual production choice) |

SASL login happens with **protocol requests** before anything else: `SaslHandshake` (choose the mechanism: PLAIN,
SCRAM-SHA-512, OAUTHBEARER, GSSAPI/Kerberos), then `SaslAuthenticate` (the credentials). After that the connection
belongs to a **principal** (`User:orders-service`), and every request is checked against **ACLs**.

With TLS, zero-copy `sendfile` is not possible (the bytes must be encrypted in the JVM), so brokers use more CPU.

---

## 14. Quotas and throttling

Brokers can limit clients by `client.id` or user: bytes/s for produce and fetch, and request time %. An over-quota
client isn't rejected. The broker **delays** its response and sets `throttle_time_ms` in it, and the client then
waits that long before sending again. Metrics: `produce-throttle-time-avg`, `fetch-throttle-time-avg`.

---

## 15. What the article says vs reality

| The article says | Reality |
|---|---|
| binary is more compact and faster to parse than text | ✔ true. Fixed-size ints, no parsing of text numbers, little garbage |
| binary allows "easier schema evolution through added fields" | only with **versions** (or tagged fields). simple-kafka has neither, so adding a field breaks old clients |
| binary enables zero-copy | zero-copy comes from the **record format on disk being the same as on the wire**, so the broker can send file bytes unchanged. Binary alone isn't enough; simple-kafka copies everything |
| each request starts with a type byte | real requests start with a **size**, then a 2-byte api key **and a version** |

---

## 16. simple-kafka vs real Kafka

| | simple-kafka | real Kafka |
|---|---|---|
| Framing | none: the reader hopes the whole request arrives in one read | int32 size prefix on every message |
| Request type | 1 byte (`PRODUCE = 0x01`) | int16 api key |
| Versions | none | per API, negotiated with ApiVersions |
| Matching responses | one request at a time per connection | correlation id + pipelining (5 in flight) |
| Client identity | none | `client_id`, SASL principal |
| Unit of data | one message `byte[]` | record batches for many partitions per request |
| Errors | text message | int16 code per partition, retriable or not |
| Optional fields | impossible | tagged fields |
| Security | none | TLS, SASL, ACLs |
| Definition | hand-written encode/decode | JSON schemas → generated code |

---

## 17. Try it

```bash
# which API versions does each broker support?
kafka-broker-api-versions.sh --bootstrap-server <broker>:9092

# watch the protocol on a PLAINTEXT listener; Wireshark has a Kafka dissector (filter: kafka)
sudo tcpdump -i any -w kafka.pcap port 9092
```

In the Java client, set the logger `org.apache.kafka.clients.NetworkClient` to DEBUG to see every request sent
(api key, version, correlation id, destination broker).

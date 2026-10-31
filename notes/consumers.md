# Consumers (how real Kafka works)

General concepts about consumers, described the way **real Kafka** (Java client) works. Method explanations live in
the `stage-N-*.md` files. Related: `protocol.md` (requests), `brokers.md` (coordinators), `partition.md` (offsets, HW).
Quiz: `quiz-1-protocol-and-clients.md`, `quiz-4-cluster-internals.md`

Contents:
1. What a consumer is · 2. The poll loop · 3. Inside `poll()`: fetching · 4. Position vs committed offset ·
5. Consumer groups · 6. The group coordinator · 7. The classic rebalance protocol · 8. Timeouts: how a consumer
is declared dead · 9. Assignors · 10. Eager vs cooperative rebalancing · 11. Static membership ·
12. The new consumer protocol (KIP-848) · 13. Committing offsets · 14. Where the consumer resumes after a restart ·
15. Delivery guarantees · 16. Rebalance listeners · 17. Threading · 18. Spring Kafka · 19. Common problems ·
20. Lag and monitoring · 21. simple-kafka vs real Kafka

---

## 1. What a consumer is

A **consumer** is a client that **reads records from partitions**, in offset order, by sending Fetch requests to
partition leaders. In Java it's `KafkaConsumer<K, V>`.

Three things to know first:
- **Pull, not push.** The consumer asks for data at its own speed. The broker never pushes. A slow consumer simply
  falls behind (lag). It doesn't slow down the broker or the producers.
- **The consumer chooses its position.** It can start anywhere, re-read old data (`seek`), and many groups can read the
  same partition independently.
- **`KafkaConsumer` is not thread-safe.** One instance = one thread. (The only method you may call from another thread
  is `wakeup()`.)

---

## 2. The poll loop

```java
Properties p = new Properties();
p.put("bootstrap.servers", "kafka-0:9092");
p.put("group.id", "billing-service");
p.put("key.deserializer", StringDeserializer.class.getName());
p.put("value.deserializer", StringDeserializer.class.getName());
p.put("enable.auto.commit", "false");

try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(p)) {
    consumer.subscribe(List.of("orders"));                 // join the group, get partitions
    while (running) {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
        for (ConsumerRecord<String, String> r : records) {
            process(r.key(), r.value());                    // your code
        }
        consumer.commitSync();                              // "I'm done up to here"
    }
}                                                           // close(): commit (if auto), LeaveGroup
```

`poll()` does much more than "get records". It also **keeps group membership alive**, **runs rebalances** and calls
your rebalance listener, sends **auto-commits**, and **refreshes metadata**. Not calling `poll()` often enough gets you
kicked out of the group (section 8).

`subscribe(topics)` = "the group decides which partitions I get". `assign(partitions)` = "I choose exact partitions, no
group membership, no rebalances" (used by tools and some stream processors).

---

## 3. Inside `poll()`: fetching

```
poll()
 ├─ coordinator stuff: heartbeat due? rebalance? auto-commit due?
 ├─ records already in the buffer? → return up to max.poll.records (500) of them
 └─ otherwise: Fetch to every leader of my assigned partitions (one request per broker)
       Fetch(orders-0 @ 1500, orders-3 @ 880, …, max 50 MB, min 1 byte, wait ≤ 500 ms)
       ← batches → decompress, check CRC, deserialize → buffer → return up to 500
 └─ then immediately send the NEXT fetch in the background (pre-fetch) while your code processes
```

| Config | Default | Effect |
|---|---|---|
| `fetch.min.bytes` | 1 | broker answers as soon as there's ≥ 1 byte. Raise it for fewer, bigger fetches |
| `fetch.max.wait.ms` | 500 | max time the broker holds the fetch (long poll) |
| `max.partition.fetch.bytes` | 1 MB | per partition per fetch (at least one batch is always returned) |
| `fetch.max.bytes` | 50 MB | per fetch response |
| `max.poll.records` | 500 | max records **returned by one `poll()`**. It doesn't change what's fetched |
| `isolation.level` | read_uncommitted | `read_committed` = skip aborted transactions, stop at the LSO |

The consumer reads only **below the high watermark**, so it never sees records that could still be lost
(`partition.md`, section 5).

---

## 4. Position vs committed offset

Two different numbers per partition:

| | What | Where | Changes when |
|---|---|---|---|
| **position** | offset of the next record `poll()` will return | in the consumer's memory | every `poll()` |
| **committed offset** | offset where the **group** continues after a restart or rebalance | `__consumer_offsets` on the broker | only when you commit |

```
orders-0:  ... 97  98  99  100 101 102 103 104 105 ...
                        ↑               ↑           ↑
                 committed = 100   position = 104   HW = 106
           (restart resumes here) (next poll returns 104)
```

Records 100–103 have been returned by `poll()` but not committed. If this consumer crashes now, the next owner of
`orders-0` starts at 100 and processes 100–103 **again**.

The committed offset is the **next** offset to read (last processed + 1), not the last processed.

---

## 5. Consumer groups

A **consumer group** = consumers with the same `group.id` sharing the work of reading topics. Kafka splits the
partitions among the members, and **each partition goes to exactly one member** of the group:

```
topic orders (6 partitions), group "billing-service"
  consumer A: orders-0, orders-1
  consumer B: orders-2, orders-3
  consumer C: orders-4, orders-5

another group "analytics" reads the SAME partitions independently, with its own offsets
  consumer X: orders-0 … orders-5
```

- **Inside a group**: work is divided (like a queue). Max useful members = number of partitions.
- **Between groups**: everyone gets everything (like publish/subscribe).
- When a member joins, leaves or dies, or the partition count changes, partitions are redistributed: a **rebalance**.

---

## 6. The group coordinator

Every group is managed by one broker, its **group coordinator**: the leader of partition
`abs(hash(group.id)) % 50` of `__consumer_offsets` (`brokers.md`, section 10).

```
1. consumer → FindCoordinator("billing-service") → any broker
2. ← "broker 2" (leader of __consumer_offsets-17)
3. all group traffic (join, heartbeat, commit, offset fetch) goes to broker 2
4. broker 2 dies → __consumer_offsets-17 gets a new leader → the new leader loads the group's state from that
   partition → consumers get NOT_COORDINATOR → FindCoordinator again → continue
```

The coordinator knows the members, the **generation** (a number that goes up with every rebalance), each member's
assignment, and the committed offsets. It doesn't fetch records for the group. Fetches go to the partition leaders.

---

## 7. The classic rebalance protocol

(The default before Kafka 4.0, and still common. The new protocol is in section 12.)

```
consumers A and B are in the group (generation 4). Consumer C starts.

C → JoinGroup(group, subscriptions [orders], assignors [range, cooperative-sticky])
coordinator: "rebalance!" → tells A and B in their next heartbeat response: REBALANCE_IN_PROGRESS
A, B → JoinGroup (from inside their poll())
coordinator waits until all known members joined (max: max.poll.interval.ms), then:
  generation = 5, picks a GROUP LEADER (e.g. A), chooses an assignor all support
  → JoinGroup response to A: "you're the leader, here are all members and their subscriptions"
  → JoinGroup response to B, C: "you're a member"
A (the leader, a CONSUMER, not the broker!) runs the assignor:
  A: orders-0,1   B: orders-2,3   C: orders-4,5
A → SyncGroup(generation 5, the assignments)
B, C → SyncGroup(generation 5, empty)
coordinator → each SyncGroup response: "your partitions are …"
everyone: fetch committed offsets for their partitions (OffsetFetch) → start fetching
```

Two points:
- **The assignment is computed by a client**, the group leader, not by the broker. That's why you can plug in custom
  assignors without changing brokers. With the new protocol (section 12) this moves to the broker.
- The **generation** fences old members: a commit with generation 4 after the rebalance gets `ILLEGAL_GENERATION`.

On the broker, `group.initial.rebalance.delay.ms` (3 s) delays the first rebalance of a new group, so 10 consumers starting
together cause one rebalance instead of 10.

---

## 8. Timeouts: how a consumer is declared dead

There are **two different checks**, because "the process is alive" and "the process is making progress" are different things:

| Check | Config | Default | Detects |
|---|---|---|---|
| **Heartbeats** from a background thread | `heartbeat.interval.ms` / `session.timeout.ms` | 3 s / 45 s | the process or machine died, or the network broke |
| **Time between `poll()` calls** | `max.poll.interval.ms` | 5 min | the process is alive but **stuck or too slow** processing |

```
Case 1: pod killed with kill -9
   no heartbeats → after 45 s the coordinator removes it → rebalance → its partitions move

Case 2: processing a batch takes 7 minutes (slow DB)
   heartbeats are fine (background thread), but poll() wasn't called for > 5 min
   → the consumer itself leaves the group → rebalance → its partitions go to someone else
   → when it finally calls commitSync(): CommitFailedException
   → the other consumer processes the same records again → duplicates
```

Fixes for case 2: process faster, lower `max.poll.records` (fewer records per poll = less work between polls), or raise
`max.poll.interval.ms`.

---

## 9. Assignors

`partition.assignment.strategy` (default since 3.0: `[RangeAssignor, CooperativeStickyAssignor]`). The group uses the
**first strategy in the list that every member supports**, so with the default list it's Range. To switch to
cooperative-sticky, you remove Range from the list with a rolling restart.

Example: topics `orders` and `payments`, 3 partitions each, consumers A and B.

| Assignor | Result | Notes |
|---|---|---|
| **Range** (per topic) | A: orders-0,1 payments-0,1 · B: orders-2 payments-2 | the same partition numbers go together (good for joining topics by key); first consumers get more |
| **RoundRobin** | A: orders-0,2 payments-1 · B: orders-1 payments-0,2 | balanced across topics |
| **Sticky** | balanced like round robin, and keeps as many partitions as possible where they were | less movement on rebalance |
| **CooperativeSticky** | sticky + cooperative protocol (section 10) | the recommended choice with the classic protocol |

---

## 10. Eager vs cooperative rebalancing

**Eager** (range, roundrobin, sticky): at the start of a rebalance, **every member gives up all its partitions**,
then gets a new assignment. During the rebalance **nobody in the group consumes anything** ("stop the world"),
even if only one partition moves.

**Cooperative** (cooperative-sticky, KIP-429): members keep their partitions. Only the partitions that must move are
revoked, and the rebalance happens in **two rounds**:
```
A: 0,1,2  B: 3,4,5   C joins
round 1: new plan A: 0,1 B: 3,4 C: 2,5 → A gives up only 2, B only 5. A and B keep consuming 0,1,3,4
round 2: 2 and 5 are free now → assigned to C
```
With 100 consumers and frequent deployments, the difference is large.

---

## 11. Static membership

Normally a restarted consumer is a **new member**. Its old member leaves (or times out), and two rebalances happen
(leave + join). With `group.instance.id` (KIP-345), set to something stable like the pod name `billing-7f9-0`:
- the consumer **doesn't send LeaveGroup** on close
- if it comes back with the same instance id **within `session.timeout.ms`**, it gets **its old partitions back, with no
  rebalance**
- good for rolling deployments on Kubernetes (StatefulSets have stable names)

Trade-off: if it really dies, its partitions are unread until `session.timeout.ms` passes. So people raise the
session timeout to minutes for static members.

---

## 12. The new consumer protocol (KIP-848)

Production-ready in **Kafka 4.0**, enabled with `group.protocol=consumer` on the client.

| | Classic | New (KIP-848) |
|---|---|---|
| Requests | JoinGroup, SyncGroup, Heartbeat | one: `ConsumerGroupHeartbeat` |
| Who computes the assignment | the group leader (a client) | the **group coordinator (broker)** (`group.remote.assignor`: uniform or range) |
| Rebalance | a group-wide barrier: everyone joins, everyone syncs | **incremental per member**: each member moves to its new assignment independently, through its heartbeats |
| One slow member | delays the whole group's rebalance | affects only itself |
| Timeouts | `session.timeout.ms`, `heartbeat.interval.ms` on the client | set on the broker (`group.consumer.session.timeout.ms` 45 s, `group.consumer.heartbeat.interval.ms` 5 s) |
| Config on the client | `partition.assignment.strategy` | not used |

```
member C heartbeats: "I'm new, I subscribe to orders"
coordinator computes a new target: A: 0,1 B: 2,3 C: 4,5 (epoch 6)
A's next heartbeat response: "revoke 2" → A stops 2, commits, heartbeats "done"
coordinator: 2 is free → C's heartbeat response: "you get 2" …
```

`max.poll.interval.ms` still applies: the client leaves the group if your code doesn't call `poll()`.

---

## 13. Committing offsets

A commit is an **`OffsetCommit` request to the group coordinator**. The coordinator writes a record to
`__consumer_offsets` (a **compacted** topic, RF 3), keyed by `(group, topic, partition)`, with the value
`(offset, leader epoch, metadata, timestamp)`. Compaction keeps only the latest commit per key.

| Way | How | Notes |
|---|---|---|
| **Auto** | `enable.auto.commit=true`, every `auto.commit.interval.ms` (5 s), inside `poll()` | commits what the **previous** `poll()` returned |
| **commitSync()** | blocks until the broker confirms, retries | safe, slower |
| **commitAsync(callback)** | doesn't block, **no retries** (a retry could commit an older offset after a newer one) | fast. Common pattern: `commitAsync` in the loop, `commitSync` on close/revoke |
| **commitSync(Map)** | commit specific offsets, e.g. per record or per partition | commit `record.offset() + 1` |

The commit includes the **leader epoch** of the last consumed record, so after an unclean leader election the consumer can
detect that the log was truncated under it.

Committed offsets of a group are deleted after `offsets.retention.minutes` (7 days), counted from when the group became
**empty** (KIP-211). A group that's stopped longer than that starts again from `auto.offset.reset`.

---

## 14. Where does the consumer resume after a restart?

**In simple-kafka: it doesn't remember.** `SimpleKafkaConsumer.currentOffset` is just a field in memory.
After a restart it starts again from the `startOffset` passed to the constructor (e.g. 0) and
re-reads everything. There are no commits and no consumer groups.

**In real Kafka: committed offsets.**
- The consumer periodically **commits** "for group `G`, topic `T`, partition `P`, the next offset is `N`".
- Commits are stored by the broker in an internal topic, **`__consumer_offsets`**, keyed by `(group.id, topic, partition)`.
  One broker acts as the **group coordinator** for each group.
- After a restart, the consumer joins its group, gets its partitions, asks the coordinator
  "what's my committed offset?" (`OffsetFetch`) and starts fetching from there.
- **No committed offset** (new group, or offsets expired) → `auto.offset.reset` decides: `earliest` (from the start),
  `latest` (only new records, the default), or `none` (throw an exception).
- **Committed offset no longer exists** (deleted by retention: `OFFSET_OUT_OF_RANGE`) → `auto.offset.reset` again.
- The committed offset is the **next** offset to read (last processed + 1).
- The same thing happens on a **rebalance**: the consumer that takes over a partition starts from the committed offset.
- You can override it: `seek(partition, offset)`, `seekToBeginning`, `offsetsForTimes` + `seek`, or offline with
  `kafka-consumer-groups.sh --reset-offsets` (the group must be stopped).

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
  what the *previous* `poll()` returned. A crash means up to ~5 s of records are processed again. It's at-least-once,
  **unless** you hand records to another thread: then a commit can happen before that thread is done → possible loss.
- **Manual commit** (`commitSync()` after processing): you control it; at-least-once.
- **At-least-once means duplicates will happen** → make processing **idempotent** (e.g. upsert by a unique ID).
- **Exactly-once**: Kafka transactions (consume → produce to Kafka), or store the offset in the same DB transaction as the result.

**Gotcha:** committed offsets expire if the group is empty longer than `offsets.retention.minutes`
(default 7 days). A consumer that was stopped for longer starts from `auto.offset.reset` again.

---

## 15. Delivery guarantees

### How to tell if a consumer is at-least-once or at-most-once

There is **no single setting**. It comes from the config **plus** where the code commits:

| `enable.auto.commit` | Code                                                | Result                                  |
|----------------------|-----------------------------------------------------|-----------------------------------------|
| `true`               | processes records in the poll loop (same thread)    | at-least-once                           |
| `true`               | hands records to another thread / async             | can **lose** records (no guarantee)     |
| `false`              | `commitSync()` / `commitAsync()` **after** processing | at-least-once                         |
| `false`              | commits **before** processing                       | at-most-once                            |

### Exactly-once

Only possible when the **output is also Kafka** (or you store offsets together with the result):

```
consume from "orders" → process → produce to "invoices"
   producer.beginTransaction()
   producer.send(invoices, …)
   producer.sendOffsetsToTransaction(offsets, consumer.groupMetadata())   ← the commit is part of the transaction
   producer.commitTransaction()
downstream consumers of "invoices" use isolation.level=read_committed
```
The output records and the consumer's offset commit become visible **together, or not at all**. Kafka Streams does this
with `processing.guarantee=exactly_once_v2`.

If the output is a database, the usual approach is **at-least-once + idempotent writes** (upsert by a business key), or
storing the offset in the same DB transaction and using `seek()` on startup.

---

## 16. Rebalance listeners

```java
consumer.subscribe(List.of("orders"), new ConsumerRebalanceListener() {
    public void onPartitionsRevoked(Collection<TopicPartition> parts) {
        consumer.commitSync(currentOffsets(parts));   // last chance to commit before someone else takes them
    }
    public void onPartitionsAssigned(Collection<TopicPartition> parts) {
        // e.g. load state, or seek() to an offset stored in your DB
    }
    public void onPartitionsLost(Collection<TopicPartition> parts) {
        // kicked out (e.g. max.poll.interval exceeded): DON'T commit, someone else owns them already
    }
});
```
The callbacks run **inside `poll()`**, on your thread.

---

## 17. Threading

| Model | How | Ordering per partition | Commits |
|---|---|---|---|
| **One consumer per thread** (standard) | N threads, each with its own `KafkaConsumer` in the same group | kept | simple |
| **One consumer + worker pool** | the poll thread hands records to workers | **lost** unless you route by partition/key | hard: commit only offsets whose records **all** finished |
| Parallel consumer libraries | e.g. Confluent Parallel Consumer: per-key parallelism inside a partition | per key | handled by the library |

More consumers than partitions = idle consumers. More processing parallelism than partitions needs the second or third model.

---

## 18. Spring Kafka

`@KafkaListener(topics = "orders", groupId = "billing-service", concurrency = "3")`:

- `ConcurrentMessageListenerContainer` creates **3 `KafkaConsumer`s**, each on its own thread (model 1 above).
  `concurrency` × number of pods = number of consumers in the group. More than the partition count → idle threads.
- Spring sets `enable.auto.commit=false` and **commits for you** according to `AckMode`:

| AckMode | Commits | |
|---|---|---|
| `BATCH` (default) | after all records of one `poll()` were processed by the listener | at-least-once |
| `RECORD` | after each record | at-least-once, more commits |
| `MANUAL` / `MANUAL_IMMEDIATE` | when you call `ack.acknowledge()` | you decide |
| `TIME` / `COUNT` | every N ms / N records | |

- **Errors:** the `DefaultErrorHandler` retries a failed record (default: 9 retries, no back-off = 10 attempts), then
  logs and **skips** it, or sends it to a **dead letter topic** if you configure a `DeadLetterPublishingRecoverer`.
- **Poison pills** (bytes that can't be deserialized): wrap your deserializer in `ErrorHandlingDeserializer`. Otherwise
  the consumer fails in `poll()` on the same offset forever.
- `@RetryableTopic`: non-blocking retries via retry topics (`orders-retry-0`, `orders-retry-1`, `orders-dlt`).
  The trade-off is that the order within a partition is lost for the failed records.

---

## 19. Common problems

| Symptom | Usual cause | Fix |
|---|---|---|
| frequent rebalances, `CommitFailedException` | processing between polls > `max.poll.interval.ms` | lower `max.poll.records`, faster processing, raise the interval |
| duplicates after deploys | uncommitted records reprocessed after rebalance | idempotent processing, commit on revoke, static membership, cooperative assignor |
| consumer stuck on one offset | poison pill (deserialization) or a record that always fails | `ErrorHandlingDeserializer`, DLT |
| lag grows on one partition only | hot key (one key gets most traffic) or one slow consumer | better key, more partitions don't help a single hot key |
| a new consumer group reads nothing | `auto.offset.reset=latest` (default) | `earliest` if you need old data |
| an old group re-reads everything / skips everything | offsets expired (7 days empty) or deleted by retention | monitor, adjust `offsets.retention.minutes` |
| idle consumers | more consumers than partitions | more partitions, or fewer consumers |

---

## 20. Lag and monitoring

**Lag** = high watermark − committed offset, per partition. It's how far behind the group is.

```
kafka-consumer-groups.sh --bootstrap-server <host:9092> --describe --group <group.id>
GROUP            TOPIC   PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG   CONSUMER-ID       HOST
billing-service  orders  0          1500            1520            20    consumer-1-ab…    /10.0.0.5
billing-service  orders  1          880             880             0     consumer-2-cd…    /10.0.0.6
```
`CURRENT-OFFSET` = committed offset, `LOG-END-OFFSET` = HW (despite the name), `LAG` = the difference.

```bash
kafka-consumer-groups.sh --bootstrap-server <host:9092> --describe --group <g> --members --verbose   # who has what
kafka-consumer-groups.sh --bootstrap-server <host:9092> --describe --group <g> --state               # Stable / PreparingRebalance / Empty
kafka-consumer-groups.sh --bootstrap-server <host:9092> --group <g> --reset-offsets --to-earliest --topic orders --execute
```

Client metrics: `records-lag-max`, `records-consumed-rate`, `commit-latency-avg`, `rebalance-rate-per-hour`,
`last-poll-seconds-ago`.

Lag in **records** can mislead (100 small records vs 100 huge ones). What matters is lag in **time**: how old the oldest
unprocessed record is.

---

## 21. simple-kafka vs real Kafka

| | simple-kafka | real Kafka |
|---|---|---|
| Position | `currentOffset` field in memory | position in memory + committed offset on the broker |
| Restart | starts from the constructor's `startOffset` | resumes from the committed offset |
| Groups | none: one consumer reads one chosen partition | consumer groups, coordinator, rebalances |
| Failure detection | none | heartbeats + `max.poll.interval.ms` |
| Fetching | one partition, one request, wait for the answer | all assigned partitions, one request per broker, pre-fetch, long poll |
| Reads up to | LEO | high watermark (or LSO) |
| Delivery guarantee | effectively at-most-once / at-least-once depending on where you restart | at-least-once by default, exactly-once with transactions |

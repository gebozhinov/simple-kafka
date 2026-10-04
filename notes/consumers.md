# Consumers

General concepts about consumers. Method explanations live in the `stage-N-*.md` files.

---

## Where does the consumer resume after a restart?

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

---

## How to tell if a consumer is at-least-once or at-most-once

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

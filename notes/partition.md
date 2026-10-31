# Partitions (how real Kafka works)

General concepts, described the way **real Kafka** works. simple-kafka is mentioned only as a comparison.
Method explanations of our `Partition` class live in `stage-4-storage.md`.
Quiz: `quiz-3-storage.md`

Contents:
1. What a partition is · 2. The units: topic → partition → replica → segment → batch → record ·
3. Why partitions exist · 4. Choosing a partition · 5. Offsets · 6. Creating partitions ·
7. Files on disk · 8. Record batch format · 9. The offset index · 10. The time index · 11. Rolling segments ·
12. Write path · 13. Replication and the high watermark · 14. Read path · 15. Retention · 16. Compaction ·
17. Crash recovery and truncation · 18. Tiered storage · 19. How many partitions · 20. simple-kafka vs real Kafka ·
21. Try it on your work cluster

---

## 1. What is a partition?

**A partition is an ordered, append-only log of records, stored as files on the disks of a few brokers.**
Each record gets a number, its **offset** (0, 1, 2, …), in the order the leader received it.

```
orders, partition 0
offset:   0        1        2        3        4
        ┌────────┬────────┬────────┬────────┬────────┐
        │ order  │ order  │ order  │ order  │ order  │  ← new records go here
        │ #1001  │ #1002  │ #1003  │ #1004  │ #1005  │
        └────────┴────────┴────────┴────────┴────────┘
        oldest                                newest
```

Three rules define it:
1. **Append-only.** Records are only added at the end. You never insert in the middle.
2. **Immutable.** A written record is never changed. "Update order #1001" = a **new** record.
3. **Ordered.** Inside one partition, every consumer reads the records in the same order.

That's why Kafka is called a **log**. It's not a queue: reading doesn't remove anything. Records stay until
**retention** deletes them, and any number of consumer groups can read them, each at their own position.

---

## 2. The units

```
Cluster
└── Topic "orders"                          a name + config (retention, …). No data of its own
     ├── Partition 0  (orders-0)            the ordered log. Offsets count per partition
     │    ├── Replica on broker 1  ← LEADER     takes all writes, serves reads
     │    │    ├── Segment 00000000000000000000   .log .index .timeindex  (closed, read-only)
     │    │    │    ├── Record batch  offsets 0..499     ← unit of writing, storing, replicating, sending
     │    │    │    │    ├── Record  offset 0  (key, value, headers, timestamp)
     │    │    │    │    └── Record  offset 1 …
     │    │    │    └── Record batch  offsets 500..509
     │    │    └── Segment 00000000000000042000   (active: the only one written to)
     │    ├── Replica on broker 2  ← follower (in ISR)    same bytes, same offsets
     │    └── Replica on broker 3  ← follower (in ISR)
     ├── Partition 1
     └── Partition 2
```

| Unit | What it is | Identified by |
|---|---|---|
| **Topic** | a name and config; a group of partitions | name `orders` (+ a topic id, a UUID) |
| **Partition** | one ordered log | `orders-0` (topic + partition number) |
| **Replica** | one copy of a partition on one broker | (partition, broker id) |
| **Segment** | one piece of a replica's log on disk | base offset in the file name |
| **Record batch** | a group of records written together | base offset … last offset |
| **Record** | one message: key, value, headers, timestamp | offset |

- **Leader:** exactly one replica per partition. All produce requests go to it, and by default all fetches too.
- **Followers:** fetch from the leader to stay in sync. They take over if the leader dies.
- **ISR (in-sync replicas):** the leader plus the followers that are caught up (see section 13).
- **Preferred leader:** the first replica in the list. Kafka moves leadership back to it to keep load balanced
  (`auto.leader.rebalance.enable=true`).

The **batch** is the unit you'll miss if you only look at the client API. The producer sends batches, the
broker stores them **as they are** (it doesn't unpack them), followers copy them, and consumers receive them.

---

## 3. Why partitions exist

### Scale: one topic spread over many machines

A topic with 1 partition can be no bigger and no faster than **one disk on one broker**. With 6 partitions on 3
brokers, writes and storage are spread out:

```
broker 1: orders-0 (L), orders-3 (L), orders-1 (F), orders-4 (F)
broker 2: orders-1 (L), orders-4 (L), orders-2 (F), orders-5 (F)
broker 3: orders-2 (L), orders-5 (L), orders-0 (F), orders-3 (F)
```

### Parallel consumers

Inside a consumer group, **each partition is read by exactly one consumer**. So the number of partitions is the
**maximum parallelism**:

```
6 partitions, 3 consumers → 2 partitions each
6 partitions, 6 consumers → 1 each
6 partitions, 8 consumers → 6 work, 2 sit idle
```

### Ordering where you need it

Kafka guarantees order **within one partition only**. You choose a **key** so related records go to the same
partition:

```
key = customerId
  customer 42: created → paid → shipped   all go to partition 1, read in this order ✔
  customer 77: created → paid             all go to partition 4
  Order between customer 42 and customer 77: not guaranteed (and not needed)
```

Ordering also needs the producer to not reorder on retries. With `enable.idempotence=true` (the default since
Kafka 3.0), order is kept even with `max.in.flight.requests.per.connection` up to 5.

---

## 4. Choosing a partition

The **producer** decides, before sending (`Partitioner`):

| Case | Java client (3.3+) | simple-kafka |
|---|---|---|
| `ProducerRecord` has a partition | use it | `send(message, partition)` |
| has a key | `toPositive(murmur2(keyBytes)) % numPartitions` | — |
| no key | **sticky**: fill one batch for one partition, then switch. Busier, slower brokers get less | `random.nextInt(partitionCount)` |

Sticky exists because spreading keyless records one by one makes many small batches, and small batches are slow.

**Adding partitions changes `% numPartitions`.** Customer 42 was on partition 1 with 6 partitions, and may be on
partition 3 with 8. Old records stay on 1 and new ones go to 3, so a consumer may process "shipped" (on 3) before
"paid" (on 1). Kafka allows **increasing** the count, never decreasing it (that would mean deleting a log).
So plan the count up front.

---

## 5. Offsets

```
offset:  0 … 41999 | 42000 … 99   100   101  102  103  104  105
        [deleted  ] [■] … [■]   [■]   [■]  [■]  [■]  [■]  [ ]
                     ↑            ↑          ↑         ↑    ↑
              log start      committed     last      high  log end
               offset        offset of    stable  watermark  offset
                             group "app"  offset             (LEO)
```

| Name | Meaning | Where it lives |
|---|---|---|
| **Log start offset** | oldest offset still available. Retention, `deleteRecords` and compaction move it forward | `log-start-offset-checkpoint` |
| **Log end offset (LEO)** | the offset the **next** record will get. Each replica has its own LEO | the log itself (last batch) |
| **High watermark (HW)** | everything below it is on **all ISR replicas** = "committed". Consumers can read only below it | `replication-offset-checkpoint` |
| **Last stable offset (LSO)** | ≤ HW. Below it there are no open transactions. `read_committed` consumers stop here | computed |
| **Committed offset** | where a consumer group continues. **Not** in the partition: stored in the `__consumer_offsets` topic by the group coordinator | `__consumer_offsets` |

**Consumer lag** = HW (shown as "LOG-END-OFFSET" in `kafka-consumer-groups.sh`) − committed offset.

**Why the HW matters.** The leader has 0..104, but follower 2 has copied only up to 101. If consumers could read 102..104
and the leader then died, follower 2 would become leader **without** them. Consumers would have processed records
that no longer exist. Records below the HW are on all ISR replicas, so they survive any single failure.

Offsets are **never reused** in a partition. They only go up, even after records are deleted. After compaction,
offsets have **gaps** (101, 103, 107, …), so consumers must never assume `next = previous + 1`.

---

## 6. Creating partitions

### Who creates them in real Kafka (KRaft)

```
1. Admin:      kafka-topics.sh --create --topic orders --partitions 3 --replication-factor 3
               (or AdminClient.createTopics, or a producer/consumer if auto.create.topics.enable=true)
               → CreateTopics request, sent to any broker, forwarded to the active controller
2. Controller: validates the request, then picks replicas for each partition, spreading leaders and
               racks (broker.rack):
                 orders-0 → replicas [1, 2, 3]  leader 1  isr [1, 2, 3]  leaderEpoch 0
                 orders-1 → replicas [2, 3, 1]  leader 2
                 orders-2 → replicas [3, 1, 2]  leader 3
               and appends TopicRecord + 3 × PartitionRecord to the metadata log (__cluster_metadata)
3. Every broker replays the metadata log. ReplicaManager sees the change:
               "I'm a replica of orders-0" → LogManager.getOrCreateLog
                   → mkdir <log.dirs>/orders-0/
                   → empty segment 00000000000000000000.{log,index,timeindex}
                   → partition.metadata (topic id)
               leader 1: makeLeader → accepts produce requests
               brokers 2, 3: makeFollower → a fetcher thread starts fetching orders-0 from broker 1
```

The controller **decides**. Each broker **creates files only for its own replicas**. Broker 4 has no `orders-0`
directory at all.

With several disks in `log.dirs`, the broker puts a new partition on the directory with the **fewest partitions**.

Other ways partitions appear or move:
- **Adding partitions:** `kafka-topics.sh --alter --partitions 6`. New, empty logs; existing data isn't moved.
- **Reassignment:** `kafka-reassign-partitions.sh` moves a replica to another broker. The new broker creates the
  directory and copies the whole log from the leader, and then the old replica is deleted.

### After a restart

Nothing is created. `LogManager` finds the existing directories and loads them (and recovers them if the shutdown was
unclean, see section 17).

### In simple-kafka

The controller's `createTopic()` assigns leaders round robin and stores them in ZooKeeper. **Every** broker then creates
a `Partition` for **every** partition, even ones it isn't a replica of (their directories stay empty).

---

## 7. Files on disk

```
/var/lib/kafka/data/                           ← log.dirs  (default /tmp/kafka-logs!)
├── orders-0/
│   ├── 00000000000000000000.log               record batches, offsets 0..41999
│   ├── 00000000000000000000.index             offset → position (sparse)
│   ├── 00000000000000000000.timeindex         timestamp → offset (sparse)
│   ├── 00000000000000042000.log               ← active segment
│   ├── 00000000000000042000.index             (preallocated 10 MB while active)
│   ├── 00000000000000042000.timeindex
│   ├── 00000000000000042000.snapshot          producer state at offset 42000 (idempotence)
│   ├── 00000000000000042000.txnindex          aborted transactions (only if transactions are used)
│   ├── leader-epoch-checkpoint                which leader epoch started at which offset
│   └── partition.metadata                     topic id
├── orders-1/
├── __consumer_offsets-17/                     committed offsets are a topic too (50 partitions)
├── recovery-point-offset-checkpoint           per partition: everything below this was fsynced
├── replication-offset-checkpoint              per partition: high watermark
├── log-start-offset-checkpoint                per partition: log start offset
├── cleaner-offset-checkpoint                  per partition: how far compaction got
├── meta.properties                            cluster id + node id (written by kafka-storage format)
└── .kafka_cleanshutdown                       exists only after a clean shutdown
```

Temporary suffixes you may see: `.deleted` (waiting to be removed by retention), `.cleaned` / `.swap` (compaction in
progress), and `orders-0.<uuid>-delete` (the whole partition is being deleted).

`leader-epoch-checkpoint` example:
```
0            ← version
3            ← number of entries
0 0          ← epoch 0 started at offset 0
1 51230      ← epoch 1 (new leader) started at offset 51230
2 98004
```

### Why segments?

A partition can hold terabytes. One huge file would cause three problems:
- **Deleting old data** would mean rewriting the file. With segments, retention just **deletes whole old segments**,
  which is instant.
- **Recovery after a crash** would mean scanning everything. With segments, only the segments after the recovery point
  need checking.
- **Indexes** would be huge. With segments, each segment has its own small index that can be memory-mapped.

---

## 8. Record batch format (magic v2, the format since Kafka 0.11)

```
RecordBatch header (61 bytes)
  baseOffset             int64   offset of the first record ← set by the leader
  batchLength            int32   bytes after this field
  partitionLeaderEpoch   int32   leader epoch when written (used for truncation)
  magic                  int8    = 2
  crc                    uint32  CRC-32C of everything after this field
  attributes             int16   bits 0-2 compression (none/gzip/snappy/lz4/zstd), bit 3 timestamp type,
                                 bit 4 transactional, bit 5 control batch
  lastOffsetDelta        int32   last offset = baseOffset + lastOffsetDelta
  baseTimestamp          int64
  maxTimestamp           int64
  producerId             int64   ┐
  producerEpoch          int16   ├ idempotence / transactions
  baseSequence           int32   ┘
  recordsCount           int32
Records (compressed together if compression is on)
  Record
    length          varint
    attributes      int8 (unused)
    timestampDelta  varlong   timestamp = baseTimestamp + delta
    offsetDelta     varint    offset    = baseOffset + delta
    keyLength       varint (-1 = null key)
    key             bytes
    valueLength     varint (-1 = null value = tombstone)
    value           bytes
    headers         [varint count, then key/value pairs]
```

Things to notice:
- Each record stores only small **deltas**, as varints (1–2 bytes). The batch header pays for the big
  fields once.
- **Compression is per batch.** The whole records part is compressed together, so 500 similar JSON records
  compress far better than 1. The broker stores the compressed bytes **unchanged** (unless the topic's
  `compression.type` is different from the producer's) and consumers decompress them.
- The **CRC** is how the broker and the consumer detect a broken or half-written batch.
- **Control batches** (bit 5) are transaction markers (COMMIT/ABORT). They take an offset, which is one reason for
  offset gaps.

What `kafka-dump-log.sh --files 00000000000000000000.log --print-data-log` shows (the shape; values shortened):
```
baseOffset: 0 lastOffset: 2 count: 3 baseSequence: 0 lastSequence: 2 producerId: 1000 producerEpoch: 0
partitionLeaderEpoch: 0 isTransactional: false isControl: false position: 0 CreateTime: 1730390000000
size: … magic: 2 compresscodec: none crc: … isvalid: true
| offset: 0 CreateTime: 1730390000000 keySize: 2 valueSize: 5 sequence: 0 headerKeys: [] key: 42 payload: hello
| offset: 1 CreateTime: 1730390000001 keySize: 2 valueSize: 5 sequence: 1 headerKeys: [] key: 42 payload: world
| offset: 2 CreateTime: 1730390000001 keySize: 2 valueSize: 5 sequence: 2 headerKeys: [] key: 77 payload: kafka
```
One batch, three records. Compare this with simple-kafka's `[size][bytes]` per message.

---

## 9. The offset index (`.index`)

**Sparse:** one entry every `index.interval.bytes` (default 4096) of log data, not one per record.
**Entry = 8 bytes:** `[relative offset: int32][position: int32]`, where relative offset = offset − segment base offset.

```
segment 00000000000000042000.index
relative  position
   0         0          (offset 42000 at byte 0)
  37      4105          (offset 42037 at byte 4105)
  75      8230
 112     12290
```

**Lookup for offset 42080** (relative 80):
1. binary search for the largest entry ≤ 80 → `(75, 8230)`
2. scan the `.log` from byte 8230, reading only each batch's first 12 bytes (base offset + length) and jumping by
   the length, until a batch's last offset ≥ 42080
3. that batch's position is where the fetch starts

Facts that follow from this format:
- **4-byte position** → a segment can't be larger than 2 GB. That's why `segment.bytes` max is `Integer.MAX_VALUE`.
- **4-byte relative offset** → a segment rolls before its offsets go more than ~2 billion past the base.
- The active segment's index is **preallocated** (`segment.index.bytes`, 10 MB = 1,310,720 entries) and **memory-mapped**
  (`mmap`), so appending an entry is just writing to memory. On roll, it's trimmed to its real size.
- Lost or broken index? Delete it: Kafka rebuilds it from the `.log` at startup. The `.log` is the truth, and indexes are
  just helpers.

---

## 10. The time index (`.timeindex`)

**Entry = 12 bytes:** `[timestamp: int64][relative offset: int32]`. An entry is added (at most every ~4 KB) only when
the batch's max timestamp is **bigger** than the last entry's, so the timestamps in the file only go up.

Used for:
- **`offsetsForTimes`**: "first offset with timestamp ≥ 10:00". `kafka-consumer-groups.sh --reset-offsets --to-datetime` uses it.
- **Time-based retention:** the segment's newest timestamp = its time index's last entry.
- **Time-based roll** (`segment.ms`).

The timestamp is the producer's `CreateTime`, or the broker's time if the topic has
`message.timestamp.type=LogAppendTime`. A producer with a wrong clock can therefore affect time-based retention.

---

## 11. Rolling segments

The active segment is closed and a new one started (at base offset = LEO) when, before an append:

| Condition | Config (topic / broker) | Default |
|---|---|---|
| the batch doesn't fit | `segment.bytes` / `log.segment.bytes` | 1 GB |
| the segment is old | `segment.ms` / `log.roll.ms` (+ random `segment.jitter.ms`) | 7 days |
| the offset or time index is full | `segment.index.bytes` | 10 MB |
| relative offset overflow | — | ~2^31 offsets |

Retention and compaction never touch the **active** segment, so `segment.ms` / `segment.bytes` also decide how
**fast** retention can act. A topic with `retention.ms=1h` but 1 GB segments and little traffic will keep old data for
days, because the active segment never fills.

---

## 12. Write path (produce)

```
Producer: send(orders, key=42) → partitioner → orders-1 → metadata says leader = broker 2
          → records collected into a batch (batch.size 16 KB, linger.ms 5 since 4.0; was 0)
          → Produce request to broker 2
   │
   ▼
Broker 2 (leader of orders-1)
   1. Partition: am I leader? acks=all and ISR size < min.insync.replicas? → NOT_ENOUGH_REPLICAS, nothing written
   2. LogValidator: CRC ok? size ≤ max.message.bytes? timestamps ok? (recompress if compression.type differs)
   3. Idempotence: producerId 1000 sequence 7, expected 7 ✔ (a duplicate retry → return the original offset)
   4. Assign offsets: LEO = 500 → baseOffset 500, lastOffsetDelta 9 → offsets 500..509, LEO = 510
   5. Maybe roll the segment. Append the batch bytes to the active .log → OS page cache. NO fsync
   6. Index entry if ≥ 4 KB since the last one; time index entry if a new max timestamp
   │
   ├── acks=0   producer didn't wait at all
   ├── acks=1   respond now: "OK, baseOffset 500"
   └── acks=all park the request in the "produce purgatory" until HW ≥ 510 (or request.timeout.ms → error)
   │
   ▼  followers 1, 3: Fetch(orders-1, offset 500) → get the same bytes → appendAsFollower (same offsets)
   │  next Fetch(offset 510) tells the leader "I have everything below 510"
   ▼
Leader: all ISR replicas have LEO ≥ 510 → HW = 510 → purgatory completes → "OK, baseOffset 500"
```

**Durability comes from replication, not fsync.** After step 5 the data is only in RAM. With `acks=all` it's in RAM on
all ISR brokers. To lose it, all of them would have to lose power before the OS writes the page cache to disk. The OS
writes dirty pages within seconds anyway. Kafka fsyncs on segment roll and on clean shutdown, plus whenever
`flush.messages`/`flush.ms` say so (not set by default).

---

## 13. Replication and the high watermark

Followers are **consumers of the leader**. A fetcher thread on each follower sends `Fetch(partition, fetchOffset)`
in a loop, and `fetchOffset` = the follower's LEO. So the leader always knows how far each follower has got.

### Example: HW over time (RF 3, ISR {1, 2, 3}, leader 1)

```
                     leader 1 LEO   follower 2 LEO   follower 3 LEO   HW (= min LEO of ISR)
start                    500            500              500            500
producer writes 10       510            500              500            500
2 fetches(500)→gets 10   510            510              500            500   ← leader learns 2 has 510 on its NEXT fetch
2 fetches(510)           510            510              500            500
3 fetches(500), (510)    510            510              510            510   → acks=all producer gets OK
```

Followers learn the HW from the fetch **response**, so a follower's HW is always one round behind the leader's.

### The ISR

A follower is **in sync** if it has caught up to the leader's LEO within the last `replica.lag.time.max.ms`
(default 30 s). If it falls behind longer (it's dead, slow disk, long GC), the leader **removes it from the ISR**
(an "ISR shrink", written to the metadata log by the controller). When it catches up again, it's **added back**.

```
min.insync.replicas=2, RF 3:
ISR {1,2,3} → acks=all writes OK
ISR {1,2}   → still OK (2 ≥ 2), HW now waits only for 1 and 2
ISR {1}     → acks=all writes rejected: NOT_ENOUGH_REPLICAS. acks=1 writes still accepted (and risky)
```

### Leader election

When the leader dies, the controller picks a new leader **from the ISR** and increments the **leader epoch**.
Every replica in the ISR has all committed records (below the HW), so nothing committed is lost.

If **no** ISR replica is alive: with `unclean.leader.election.enable=false` (the default) the partition stays
**offline** until an ISR replica comes back. With `true`, an out-of-sync replica becomes leader, so the partition is
available again but the records it didn't have are **lost**.

---

## 14. Read path (fetch)

```
Consumer: Fetch(orders-1, offset 505, max.partition.fetch.bytes 1 MB, fetch.max.wait.ms 500) → leader
   1. offset < log start offset or > LEO? → OFFSET_OUT_OF_RANGE → consumer applies auto.offset.reset
   2. upper limit: HW (read_uncommitted) or LSO (read_committed). Followers: LEO
   3. offset ≥ limit? → wait up to fetch.max.wait.ms for new data ("long poll"), then return what there is
   4. segments.floorEntry(505) → segment 0; index lookup + scan → batch 500..509 at position 81920
   5. result = FileRecords slice (file, position 81920, length up to 1 MB), ending at a whole batch
   6. network layer: FileChannel.transferTo → sendfile(): page cache → socket, no copy into the JVM (zero-copy)
   │
   ▼
Consumer: decompress the batch, check the CRC (check.crcs=true), skip 500..504, return 505..509 from poll()
```

- **Whole batches only.** The broker never splits a batch.
- **At least one batch:** the first batch is returned even if it's bigger than the max bytes, so a large record can't
  block a consumer.
- **One segment at a time:** a fetch doesn't cross a segment boundary.
- **Zero-copy needs plain TCP.** With TLS the bytes must be encrypted, so they go through the JVM.
- **Follower fetching (KIP-392):** with `replica.selector.class` set and a consumer `client.rack`, consumers can
  read from a follower in their own data centre or availability zone, which reduces cross-zone traffic costs.

### Why Kafka is fast even though it stores to disk

1. **Sequential I/O**: writes always go at the end of a file, reads are long scans.
2. **Page cache**: recent data is still in OS memory. Consumers that keep up read from RAM, not disk.
   That's why Kafka brokers need a small JVM heap (~6 GB) and lots of free RAM for the OS.
3. **Zero-copy** (`sendfile`) from page cache to socket.
4. **Batching + compression** end to end: one write, one network call, one disk block for many records.
5. **No per-message state on the broker**: consumers keep their own position. The broker doesn't track "read/unread"
   per message like a classic queue does.

---

## 15. Retention (`cleanup.policy=delete`)

Every `log.retention.check.interval.ms` (5 min), `LogManager` checks each partition and deletes **whole segments**
from the oldest end:

| Rule | Config | Default |
|---|---|---|
| time: the segment's **newest** timestamp is older than | `retention.ms` / `log.retention.hours` | 7 days |
| size: the partition is bigger than | `retention.bytes` (per partition!) | -1 (no limit) |
| explicit | `kafka-delete-records.sh` / `AdminClient.deleteRecords` | — |

Deletion steps: move the log start offset forward → rename the files to `.deleted` → actually delete them after
`file.delete.delay.ms` (60 s), so current readers can finish.

```
retention.ms = 7 days
segment 0      records from day 1–3, newest 10 days old → deleted
segment 42000  records from day 3–8, newest 5 days old  → kept, although some records are 9 days old
segment 90000  active                                    → never deleted
```

So retention is a **minimum, not exact**. Data lives at least `retention.ms`, plus up to one segment's worth.

After segment 0 is deleted, the log start offset is 42000. A consumer whose committed offset is 100 gets
`OFFSET_OUT_OF_RANGE` and follows `auto.offset.reset`: `earliest` → 42000 (records 100..41999 were never read
= data loss for that consumer), `latest` → LEO (skips everything), `none` → exception.

---

## 16. Compaction (`cleanup.policy=compact`)

Keeps at least the **latest record for every key**, and removes older records with the same key:

```
before:  off 0 k=A v=1 | off 1 k=B v=1 | off 2 k=A v=2 | off 3 k=C v=1 | off 4 k=B v=null | off 5 k=A v=3
after:   off 3 k=C v=1 | off 4 k=B v=null (tombstone, removed later) | off 5 k=A v=3
```

- Offsets **don't change**. Removed records leave gaps (3, 4, 5).
- Order is kept. Compaction only removes records.
- **Tombstone** = a record with a `null` value: "key B is deleted". It's kept for `delete.retention.ms` (24 h)
  so consumers can see the delete, then removed too.
- Run by **log cleaner threads** in the background. A partition is cleaned when its "dirty" (not yet compacted) part
  is ≥ `min.cleanable.dirty.ratio` (0.5) of the log. The **active segment is never compacted**.
- Records younger than `min.compaction.lag.ms` (0) are left alone.
- Use it for **state**, i.e. "the current value per key": `__consumer_offsets`, KTable changelogs, CDC tables,
  configuration.
- `cleanup.policy=compact,delete` = compact, **and** delete segments older than retention.

---

## 17. Crash recovery and truncation

### Recovery on the same broker (startup after a crash)

No `.kafka_cleanshutdown` marker → for each partition:
1. read the **recovery point** from `recovery-point-offset-checkpoint`. Everything below it was fsynced and is trusted.
2. for the segments after it: read each batch, **check the CRC**, rebuild `.index` / `.timeindex` and the producer state
3. at the first invalid batch: **truncate** the `.log` there. The half-written tail is gone, and LEO = the end of the last valid batch

With thousands of partitions this can take a long time. That's why controlled shutdown matters, and why
`num.recovery.threads.per.data.dir` can be raised.

### Truncation between replicas (leader epoch)

A replica can hold records that never got committed. They must be removed before it continues:

```
epoch 5, leader A, HW 8:   A: 0..9      B: 0..7       (B hadn't fetched 8, 9 yet)
A dies → B becomes leader with epoch 6 → producers write new records → B: 0..7, 8'..12' (epoch 6)
A comes back as a follower:
   A → B: OffsetsForLeaderEpoch(epoch 5)  "where did my epoch end on your side?"
   B → A: "epoch 5 ended at offset 8" (epoch 6 started at 8, from leader-epoch-checkpoint)
   A truncates to 8: drops its old 8, 9 → fetches 8'..12' from B → identical logs again
```

The records 8 and 9 on A were **never committed** (HW was 8), so no producer using `acks=all` got an OK for them.
With `acks=1` the producer **did** get an OK, and those records are now gone. This is the `acks=1` data loss from quiz 2.

Without leader epochs (before Kafka 0.11) truncation used the HW, which had rare cases of lost or diverged data.
The epoch in every batch header (`partitionLeaderEpoch`) fixed that.

---

## 18. Tiered storage (KIP-405, production-ready since Kafka 3.9)

With `remote.storage.enable=true` on a topic, **closed** segments are copied to object storage (S3, GCS, …) by the
leader. Local disks keep only recent data (`local.retention.ms` / `local.retention.bytes`). Fetches for old offsets
are served from remote storage. The partition stays one log with one offset range, but its old segments live
elsewhere, which is why segments are the unit for this too.

---

## 19. How many partitions?

| Too few | Too many |
|---|---|
| limits consumer parallelism (max consumers = partitions) | more open files, mmapped indexes and memory per broker |
| one hot partition limits throughput (one leader, one disk) | more replication work, longer recovery after unclean shutdown |
| can't grow later without breaking key order | producers spread `batch.size` across more partitions → smaller batches |

A common start: target throughput ÷ throughput of one consumer, round up, leave some room, and pick a number that
divides well (6, 12, 24). KRaft clusters can handle millions of partitions in total, but each broker should stay
in the low thousands of replicas.

---

## 20. simple-kafka vs real Kafka

| | simple-kafka | real Kafka |
|---|---|---|
| Classes | one `Partition` | `Partition` (replication) + `UnifiedLog` + `LogSegment` + indexes |
| Directory | `data/<brokerId>/<topic>/<partition>/` | `<log.dirs>/<topic>-<partition>/` |
| Who creates replicas | every broker, for every partition | only the replica brokers, from controller metadata |
| Unit on disk | one message `[4-byte size][bytes]` | record batch with CRC, timestamps, keys, headers, producer id |
| Offsets on followers | follower assigns its own | follower keeps the leader's |
| Index | dense, 16 B/entry, opened per write | sparse (4 KB), 8 B/entry, memory-mapped, + time index |
| Segment size | 1 MB, may overflow by one message | 1 GB / 7 days, never overflows |
| Flush | fsync every message (twice) | page cache; durability through replication |
| Read limit for consumers | LEO | high watermark (or LSO) |
| Big message vs max bytes | consumer stuck | first batch always returned |
| Read path | copy into JVM buffers | zero-copy `sendfile` |
| Retention / compaction | none | delete by time/size, compaction |
| Recovery | count size fields, no checks | CRC check + truncate + rebuild indexes |
| Replica divergence | not handled | leader-epoch truncation |

---

## 21. Try it on your work cluster

```bash
# partitions, leader, replicas, ISR of a topic
kafka-topics.sh --bootstrap-server <broker>:9092 --describe --topic <topic>

# topic config overrides (retention, segment size, cleanup policy)
kafka-configs.sh --bootstrap-server <broker>:9092 --describe --entity-type topics --entity-name <topic>

# size of each partition on each broker
kafka-log-dirs.sh --bootstrap-server <broker>:9092 --describe --topic-list <topic>

# earliest (= log start offset) and latest (= HW) offset of every partition
kafka-get-offsets.sh --bootstrap-server <broker>:9092 --topic <topic> --time -2
kafka-get-offsets.sh --bootstrap-server <broker>:9092 --topic <topic> --time -1

# on a broker pod: real segment files (path = the broker's log.dirs)
ls -l /var/lib/kafka/data/<topic>-0/

# decode a segment, its index and time index
kafka-dump-log.sh --files /var/lib/kafka/data/<topic>-0/00000000000000000000.log --print-data-log
kafka-dump-log.sh --files /var/lib/kafka/data/<topic>-0/00000000000000000000.index
kafka-dump-log.sh --files /var/lib/kafka/data/<topic>-0/00000000000000000000.timeindex
```

In Spring Kafka, the partition count is the upper limit for `concurrency` in `@KafkaListener(concurrency = "…")`.
Setting `concurrency` higher than the partition count just leaves threads idle.

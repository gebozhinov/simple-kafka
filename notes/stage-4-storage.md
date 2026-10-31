# Stage 4 – Storage Layer Notes (`Partition` class)

Source: https://buildthingsuseful.medium.com/stage-4-build-your-own-kafka-building-the-storage-layer-7a8497a614f3
Code: `src/main/java/com/simplekafka/broker/Partition.java`
Partition concepts (what a partition is, segments, offsets, retention, real Kafka): see `partition.md`
Quiz: `quiz-3-storage.md`

`Partition` is **one partition replica stored on one broker's disk**. It turns "append these bytes" and
"give me the messages from offset N" into reads and writes on files.

All examples below were run against this code (output and hex dumps are real).
Each method has an **In real Kafka** part that shows how the real broker does the same job.

---

## One class here = many classes in real Kafka

Our `Partition` does everything for one partition. Real Kafka splits the same work into layers:

```
ReplicaManager                 all partitions this broker hosts; handles Produce and Fetch requests
 └── Partition                 ONE partition: leader?, leader epoch, ISR, high watermark, follower progress
      └── UnifiedLog           the partition's log: assign offsets, LEO, log start offset, retention, roll
           ├── LogSegment      one segment = the files below + "append", "read", "recover"
           │    ├── FileRecords        the .log file (record batches)
           │    ├── OffsetIndex        the .index file  (offset → position, sparse)
           │    ├── TimeIndex          the .timeindex   (timestamp → offset)
           │    └── TransactionIndex   the .txnindex    (aborted transactions)
           ├── ProducerStateManager    idempotence: last sequence per producer (.snapshot files)
           └── LeaderEpochFileCache    leader-epoch-checkpoint (used to truncate followers)
LogManager                     all logs on the disk(s): create, load at startup, retention, flush, checkpoints
```

| simple-kafka `Partition` | real Kafka |
|---|---|
| `leader`, `followers`, `setLeader()` | `Partition`: leader id, **leader epoch**, ISR, set from controller metadata |
| `nextOffset`, `append()`, `readMessages()` | `UnifiedLog`: `appendAsLeader()`, `appendAsFollower()`, `read()` |
| `segments`, `createNewSegment()` | `UnifiedLog.roll()`, the segments map |
| one `.log` + `.index` | `LogSegment` with `FileRecords`, `OffsetIndex`, `TimeIndex`, `TransactionIndex` |
| `initialize()` | `LogManager` + `LogLoader` (load and recover at startup) |

So when people say "partition" in real Kafka, they mean either the **logical partition** (leader, ISR,
replicas) or its **log on disk**. Here both are one class.

## What the article says vs what the code does

The article describes the design in general terms. A few of its claims don't match the code, and it's
worth knowing which:

| The article says | What the code actually does | Real Kafka |
|---|---|---|
| the index is **sparse** | **dense**: one entry for every message (`updateIndex` in every `append`) | sparse: one entry per ~4 KB |
| a lookup falls back to the **nearest prior** entry | returns the **last** entry and doesn't scan forward (Problem 4 below) | nearest entry ≤ target, then scan the `.log` |
| reads **continue into the next segment** | intended, but broken: `ClosedChannelException` (Problem 1) | stops at the segment end; the next fetch continues |
| **zero-copy** through `FileChannel` | no: bytes are copied into a `ByteBuffer`, then a `byte[]`, then the response buffer | `FileChannel.transferTo` = `sendfile` (no TLS) |
| segments are also created during **crash recovery** | there is no recovery; a broken record is counted (Problem 3) | CRC check, truncate, rebuild indexes |
| appends are **atomic** | atomic between threads (lock), **not** between crashes (log, index and offset are written separately) | the batch is validated with a CRC; a broken tail is truncated |
| offsets are **client-managed** | the consumer keeps its position in memory | committed offsets stored by the broker in `__consumer_offsets` |

---

## The big picture first

```
Broker 1's disk
data/
└── 1/                                   ← broker id
    └── orders/                          ← topic
        ├── 0/                           ← partition 0  = one Partition object
        │   ├── 00000000000000000000.log     messages with offsets 0..13
        │   ├── 00000000000000000000.index   offset → byte position for that .log
        │   ├── 00000000000000000014.log     messages with offsets 14..   (active segment)
        │   └── 00000000000000000014.index
        ├── 1/
        └── 2/
```

- A partition's log is split into **segments**: pairs of `.log` (the messages) and `.index` (where each message starts).
- The file name is the **base offset**: the offset of the first message in that segment.
- Only the **last** segment is written to. It's called the **active segment**. Older segments are never changed.

### Who uses `Partition`

The broker class (`SimpleKafkaBroker`, next stage) keeps `Map<String, List<Partition>> topics`.

| Who | When | Calls |
|---|---|---|
| Controller, `createTopic()` | a client creates a topic | `new Partition(id, leader, followers, dir)` for each partition |
| Other brokers, `loadTopic()` | they're told a topic was created, or at startup (`loadTopics()`) | `new Partition(...)`, which loads existing files from disk |
| Leader, `handleProduceRequest()` | a producer sends a message | `append(message)` → the offset goes back to the producer |
| Follower, `handleReplicateRequest()` | the leader forwards a message | `append(message)` |
| Any broker, `handleFetchRequest()` | a consumer fetches | `getLogEndOffset()` then `readMessages(offset, maxBytes)` |
| Controller, `rebalancePartitions()` | a leader broker died | `setLeader()`, `setFollowers()` |
| Broker, `shutdown()` | stopping | `close()` |

The partition directory is built by the broker as `data/<brokerId>/<topic>/<partitionId>`.
`brokerId` is in the path so you can run 3 brokers on one laptop without them overwriting each other's files.

---

## On-disk format (this is the most important part)

### `.log` file: one record = `[size: 4 bytes][message: size bytes]`

After `append("hello")`, `append("world")`, `append("kafka")`:

```
00000000: 0000 0005 6865 6c6c 6f | 00 0000 0577 6f72 6c64 | 0000 0005 6b61 666b 61
          └ size=5 ┘└ "hello"  ┘   └ size=5 ┘└ "world" ┘     └ size=5 ┘└ "kafka" ┘
position: 0                        9                          18
```

Each record takes `4 + message.length` bytes. **The offset is not written in the `.log` file.** It's implied by the
order of the records: the 1st record in segment `0000…0000` is offset 0, the 2nd is offset 1, and so on.
The size prefix is needed because messages have different lengths. Without it, you couldn't tell where "hello" ends.

### `.index` file: one entry = `[offset: 8 bytes][position: 8 bytes]` = 16 bytes

```
00000000: 0000 0000 0000 0000  0000 0000 0000 0000   offset 0 → position 0
00000010: 0000 0000 0000 0001  0000 0000 0000 0009   offset 1 → position 9
00000020: 0000 0000 0000 0002  0000 0000 0000 0012   offset 2 → position 18 (0x12)
```

There is **one entry per message**, so entry number `k` is at byte `k * 16`. This lets you jump straight to any
offset without scanning the `.log`.

### In real Kafka

- The `.log` holds **record batches**, not single messages. Each batch header has its **base offset**, a **CRC**,
  timestamps and producer id. The offset **is** stored, so the broker never has to count records to know offsets.
- The `.index` entry is **8 bytes**: `[relative offset: 4][position: 4]`, and there is only one per ~4 KB of log.
- There are more files per segment: `.timeindex`, `.snapshot`, `.txnindex`.

The exact byte layouts are in `partition.md` ("Record batch format", "The offset index").

---

## Fields

```java
private static final int DEFAULT_SEGMENT_SIZE = 1024 * 1024; // roll to a new segment after ~1 MB
private final int id;                      // partition number: 0, 1, 2…
private int leader;                        // broker id of the leader of this partition
private List<Integer> followers;           // broker ids of the followers
private final String baseDir;              // data/1/orders/0
private final AtomicLong nextOffset;       // offset the NEXT message will get = log end offset (LEO)
private final ReadWriteLock lock;          // many readers OR one writer
private RandomAccessFile activeLogFile;    // the open active .log
private FileChannel activeLogChannel;      // channel for writing to it
private final List<SegmentInfo> segments;  // all segments, sorted by base offset
```

- `nextOffset` is the **log end offset (LEO)**. With messages 0, 1, 2 stored, `nextOffset = 3`.
- `leader` and `followers` are **metadata**. `Partition` doesn't use them itself. The broker reads them to decide
  "am I the leader?" and "who must I replicate to?". Every broker has a `Partition` object for every
  partition of a topic, even ones it doesn't lead.
- Only the **active** segment stays open. Old segments are opened only for reading, inside `readMessages`.

## Constructor

```java
public Partition(int id, int leader, List<Integer> followers, String baseDir) {
    ...
    this.nextOffset = new AtomicLong(0);
    ...
    initialize();
}
```

It stores the fields and calls `initialize()`, which sets up the files. Once constructed, the partition is
ready: `append` and `readMessages` work straight away.

Example: the controller creates partition 0 of `orders` with leader 1 and follower 2:
```java
new Partition(0, 1, List.of(2), "data/1/orders/0");
```

---

## `initialize()`

### Purpose

Prepares the partition for use. It handles two cases:
- **New partition**, with an empty directory: create segment `00000000000000000000`.
- **Broker restart**, with files already present: find the segments, work out the next offset, and reopen the last
  segment for appending. **This is how messages survive a restart.**

### Line by line

```java
File dir = new File(baseDir);
if (!dir.exists()) dir.mkdirs();
```
Creates `data/1/orders/0` (and the parents) if needed.

```java
File[] files = dir.listFiles((dir1, name) -> name.endsWith(LOG_SUFFIX));
```
Only `.log` files. The base offset comes from the file name:
`"00000000000000000014.log"` → strip `.log` → `Long.parseLong("00000000000000000014")` = `14`.

```java
if (indexFile.exists()) segments.add(new SegmentInfo(baseOffset, logPath, indexPath));
```
A `.log` without its `.index` is **ignored**. Its messages become invisible.

```java
segments.sort((s1, s2) -> Long.compare(s1.getBaseOffset(), s2.getBaseOffset()));
```
`listFiles` returns files in **no guaranteed order**. In the test run it returned
`[...14.index, ...14.log, ...0.index, ...0.log]`, so 14 came first. Sorting is required because
`findSegmentForOffset` does a binary search and `append` writes to "the last segment".

```java
nextOffset.set(lastSegment.getBaseOffset() + countMessagesInSegment(lastSegment));
```
The next offset = where the last segment starts + how many messages it has.
Example: last segment `…14`, it has 1 message → `nextOffset = 15`.
Only the last segment needs counting, because the older segments' messages are all below its base offset.

```java
if (segments.isEmpty()) createNewSegment(0);
else openSegmentForAppend(lastSegment);
```

### Example: restart

```
Run 1: append hello, world, kafka   → offsets 0,1,2, close()
Run 2: new Partition(0, 1, [2], "data/1/orders/0")
       → finds 00000000000000000000.log, counts 3 messages
       → nextOffset = 0 + 3 = 3          (test output: "after restart LEO=3")
       → the next append gets offset 3, not 0 ✔
```

### Note
All exceptions are caught and only logged. If `initialize` fails (disk full, no permission), you get
a `Partition` whose `activeLogChannel` is `null`, and the first `append` throws `NullPointerException`.
`append` only catches `IOException`, so the NPE isn't caught there.

### In real Kafka

At startup, `LogManager` loads every `<topic>-<partition>` directory in `log.dirs`, using several threads
(`num.recovery.threads.per.data.dir`, default 1). For each one, `LogLoader`:
1. deletes leftover temporary files from an interrupted operation: `.deleted`, `.cleaned`, `.swap`
2. opens every segment, sorted by base offset
3. checks whether the last shutdown was **clean**. A clean shutdown leaves a `.kafka_cleanshutdown` marker file.
   - **Clean:** the files are trusted. Only the last segment's last batch is read to get the LEO.
   - **Unclean** (crash, `kill -9`): every segment after the **recovery point** is re-read and checked (CRC), its
     indexes are rebuilt, and the file is cut off at the first broken batch.
4. LEO = the last valid batch's `baseOffset + lastOffsetDelta + 1`, read from the batch header, not counted.

If a disk fails, the broker marks that log dir **offline** and moves leadership of its partitions to other brokers.
It doesn't just log the error and keep going.

---

## `countMessagesInSegment(segment)`

### Purpose

Counts the records in a `.log` file by **hopping** from size field to size field. It reads only 4 bytes per message
and skips over the message bodies. Used only by `initialize()`.

### Example walk over the "hello/world/kafka" file (27 bytes)

```
position 0  : read 4 bytes → size 5 → jump to 0+4+5 = 9    count=1
position 9  : read 4 bytes → size 5 → jump to 18            count=2
position 18 : read 4 bytes → size 5 → jump to 27            count=3
position 27 : 27 < 27? no → stop                            → 3 messages
```

`logChannel.read(buffer)` moves the position forward by 4, and `position(position() + messageSize)` skips the body.

### Problem: a half-written last message is counted as complete

If the broker crashes in the middle of a write, the last record can be cut off. The size field says 100000, but
only 49996 bytes of the body exist. `position(...)` happily jumps **past the end of the file**, and `count++` still runs.

Tested by truncating a segment from 100004 to 50000 bytes:
```
LEO after restart = 15                       ← counts the broken message 14 as real
append returns 15                            ← new message is written right after the garbage
read 14 → WARNING: Incomplete message read   ← 0 messages, forever
read 15 → 11 bytes                           ← works, because its index entry is correct
```
A consumer at offset 14 gets 0 messages from every fetch and never moves forward. Real Kafka handles
this with **log recovery**: after an unclean shutdown it re-reads the active segment, checks each batch's CRC, and
**truncates** the file at the first broken record (see `partition.md`).

### In real Kafka

There's no counting, because offsets are stored in the batch headers. The closest real method is
`LogSegment.recover()`. It walks the segment batch by batch, the same way this hops size field to size field,
but for each batch it also:
- checks the **CRC32C**, magic byte and size
- re-adds index and time-index entries (the indexes are rebuilt from scratch)
- updates the producer state (sequence numbers)
- at the first invalid batch: **truncates** the file at that position and logs how many bytes were dropped

Applied to the test above: Kafka would cut `…14.log` back to 0 bytes (offset 14 never completed), set LEO = 14, and the
next message would get offset 14. No garbage, and no stuck consumer.

---

## `createNewSegment(baseOffset)`

### Purpose

Creates an empty `.log` + `.index` pair whose names start at `baseOffset`, adds it to `segments`, and makes it the
active segment. Called by `initialize()` for a new partition (base 0), and by `append()` when the active segment is full.

### Line by line

```java
String baseName = String.format("%020d", baseOffset);   // 14 → "00000000000000000014"
```
The name is padded with zeros to 20 digits. Why:
- `Long.MAX_VALUE` has 19 digits, so 20 is always enough.
- All names have the same length, so **alphabetical order = numeric order**. Without padding, `ls` would show
  `100.log` before `14.log`. This is the exact format real Kafka uses.

```java
logFile.createNewFile();  indexFile.createNewFile();
segments.add(segment);
openSegmentForAppend(segment);
```
`segments.add` puts it at the end. That keeps the list sorted, because a new segment always has a higher base offset.

### Example

```
segments before: [0]
append() at offset 14 notices the active .log is ≥ 1 MB
createNewSegment(14) → 00000000000000000014.log + .index, segments = [0, 14]
```

### In real Kafka

`UnifiedLog.roll()` happens when **any** of these is true for the active segment (checked before each append):
- the next batch wouldn't fit in `segment.bytes` (default 1 GB). Unlike here, the check includes the new batch,
  so the segment never goes over the limit.
- the segment is older than `segment.ms` (default 7 days)
- its offset index or time index is full
- the new batch's offset is too far from the base offset to fit in a 4-byte relative offset

On roll:
1. a new `LogSegment` is created at base offset = LEO. Its `.index` and `.timeindex` are **preallocated**
   to `segment.index.bytes` (default 10 MB) and memory-mapped.
2. the old segment's indexes are **trimmed** to their real size. The old segment is now read-only forever.
3. a producer-state `.snapshot` file is written, named by the new base offset
4. the old segment is flushed (fsynced) **in the background**. That's the main moment real Kafka fsyncs.

---

## `openSegmentForAppend(segment)`

### Purpose

Closes the current active file (if any), opens `segment`'s `.log` for reading and writing, and moves to the **end**.

```java
activeLogFile = new RandomAccessFile(segment.getLogPath(), "rw");
activeLogChannel = activeLogFile.getChannel();
activeLogChannel.position(activeLogChannel.size());
```

`"rw"` opens the file without truncating it. A new `RandomAccessFile` starts at position 0. Without
`position(size())`, the first append after a restart would **overwrite message 0**. That's why "move to the end" is
essential here.

`activeLogChannel.position()` is also used as the current file size by the rollover check in `append`.

---

## `append(message)`

### Purpose

Adds one message to the end of the partition and returns its offset (or `-1` on error). This is the write path for
both producers (on the leader) and replication (on followers).

### Line by line

```java
lock.writeLock().lock();
```
Only one writer at a time, and no readers during the write. Two producers writing at once would otherwise both get
the same `currentOffset`, and their bytes could mix.

```java
long currentOffset = nextOffset.get();          // e.g. 3
```
The offset is decided **here, by the broker**. The producer never picks it.

```java
if (activeLogChannel.position() >= DEFAULT_SEGMENT_SIZE) {
    activeLogChannel.close();  activeLogFile.close();
    createNewSegment(currentOffset);
}
```
**Rollover**: the check runs *before* writing, so a segment can grow past 1 MB by up to one message.
Test: 11 × 100 KB messages → the first segment ended at **1,100,071 bytes**. It rolled to `…14.log` only when
offset 14 arrived. The new segment's name is `currentOffset`, the offset of the first message it will hold.

```java
ByteBuffer buffer = ByteBuffer.allocate(4 + message.length);
buffer.putInt(message.length);
buffer.put(message);
buffer.flip();
long position = activeLogChannel.position();     // where this record starts → goes into the index
activeLogChannel.write(buffer);
activeLogChannel.force(true);
```
`force(true)` = **fsync**: wait until the bytes are really on the disk, not just in the OS memory cache.
It's safe but slow, roughly 1–10 ms per call on a normal disk. Real Kafka does **not** fsync per message by default:
it relies on the OS page cache plus replication to other brokers (`log.flush.interval.messages` defaults to infinite).

```java
updateIndex(currentOffset, position);           // offset 3 → position 27
nextOffset.incrementAndGet();                   // now 4
return currentOffset;                           // 3 goes back to the producer
```

### Example

```
State: messages 0,1,2 (27 bytes), nextOffset = 3
append("bulgaria".getBytes())   // 8 bytes
  currentOffset = 3
  position 27 < 1 MB → no rollover
  write [00 00 00 08]["bulgaria"] at position 27 → file is now 39 bytes
  index entry: (3, 27)
  nextOffset = 4
  return 3
```

### Notes
- The write order is: `.log` first, then `.index`, then `nextOffset`. A crash between the steps leaves the files
  inconsistent (see the `findPositionForOffset` problem below).
- On `IOException` it returns `-1`. The broker checks `offset > -1` to decide between success and error.
- **Followers call the same `append`, so they assign their own offsets.** The leader sends its offset in the
  replicate request, but the follower ignores it. If one replicate message is lost (the follower was down
  for a second), every later message has a **different offset on the follower than on the leader**. Once that
  follower becomes leader, consumers' offsets point to the wrong messages.

### In real Kafka

There are two different append methods, and this difference is essential:

**`UnifiedLog.appendAsLeader(batches)`**, on the leader:
1. `Partition.appendRecordsToLeader` first checks: am I the leader? If `acks=all`, is the ISR size ≥
   `min.insync.replicas`? If not, it returns `NOT_ENOUGH_REPLICAS` and writes nothing.
2. **Validate** each batch (`LogValidator`): CRC, size ≤ `max.message.bytes` (default ~1 MB), compression,
   and timestamps (`message.timestamp.type` = CreateTime or LogAppendTime).
3. **Assign offsets**: the batch's base offset = LEO, and the records get base + 0, + 1, … (the batch keeps one base
   offset plus small deltas).
4. **Idempotence check** (`ProducerStateManager`): is this batch's sequence number the next one expected for this
   producer id? If it's a retry of a batch already written, it returns the original offset and doesn't write it twice.
5. Maybe roll, then `LogSegment.append()`: write the batch bytes to the `.log` (to the **page cache**, no fsync),
   and add an index entry if ≥ 4 KB has been written since the last one.
6. LEO += number of records. Fsync only if `flush.messages` / `flush.ms` are set (they aren't by default).

**`UnifiedLog.appendAsFollower(batches)`**, on followers: the batches already have offsets from the leader, and
the follower **keeps them**. It never assigns offsets. So all replicas have the **same offset for the same record**.
If they ever disagree, the follower **truncates** (see "Crash recovery" in `partition.md`).

---

## `updateIndex(offset, position)`

### Purpose

Appends one 16-byte entry `(offset, position)` to the active segment's `.index`.

```java
SegmentInfo currentSegment = segments.get(segments.size() - 1);   // always the active segment
try (RandomAccessFile indexFile = new RandomAccessFile(currentSegment.getIndexPath(), "rw"); ...) {
    indexChannel.position(indexChannel.size());     // go to the end
    buffer.putLong(offset);  buffer.putLong(position);
    indexChannel.write(buffer);
    indexChannel.force(true);
}
```

It **opens and closes the index file on every message**, plus a second fsync. So each `append` costs 2 fsyncs and one
file open. This is fine for learning, but it's the main reason this would be slow.

### Our index vs Kafka's

| | simple-kafka | real Kafka |
|---|---|---|
| Entries | one per message (**dense**) | one every ~4 KB of log (**sparse**, `log.index.interval.bytes=4096`) |
| Entry size | 16 bytes (8 offset + 8 position) | 8 bytes (4 *relative* offset + 4 position) |
| Lookup | direct: entry `k` at `k * 16` | binary search for the closest entry ≤ target, then scan the `.log` forward |
| Kept in | a file opened each time | memory-mapped file (`mmap`) |

Sparse = a much smaller index (fits in RAM), at the cost of a short scan. Kafka's index stores the offset **relative**
to the segment's base offset (`offset - baseOffset`), which fits in 4 bytes because a segment never holds 4 billion
messages.

Each `append` also maybe adds a **time index** entry `(maxTimestamp so far, relative offset)`. That's what
`offsetsForTimes()` uses ("give me the first offset at or after 10:00"), and also what time-based retention uses.

---

## `readMessages(offset, maxBytes)`

### Purpose

The read path. Returns the messages starting at `offset`, in order, until `maxBytes` would be exceeded or the data runs
out. Called by the broker's `handleFetchRequest()` for consumers.

### Line by line

```java
lock.readLock().lock();
```
Many fetches can read at the same time. They wait only while an `append` holds the write lock.

```java
SegmentInfo targetSegment = findSegmentForOffset(offset);     // which file?
long position = findPositionForOffset(targetSegment, offset); // where in that file?
logChannel.position(position);
```
Two lookups: **segment** (binary search on base offsets), then **byte position** (the index).

```java
while (bytesRead < maxBytes && logChannel.position() < logChannel.size()) {
    read 4 bytes → messageSize
    if (bytesRead + messageSize > maxBytes) break;
    read messageSize bytes → add to the list
    bytesRead += messageSize + 4;
    currentOffset++;
    ...
}
```
After that it's just a **sequential read**: size, body, size, body…

### Example

```
readMessages(1, 1000) on [hello, world, kafka]
  findSegmentForOffset(1) → segment 0
  findPositionForOffset   → index entry 1 → position 9
  position 9 : size 5 → "world"   bytesRead = 9
  position 18: size 5 → "kafka"   bytesRead = 18
  position 27 = file size → stop
  → ["world", "kafka"]          (test output: "read from 1: world kafka")
```

The **broker** adds the offsets in the response: first message = requested `offset`, then +1 for each message. That works
only because the result is a continuous run of messages.

### Problem 1: reading across segments is broken (verified)

When the loop reaches the end of a segment, the "move to the next segment" block does this:

```java
logChannel.close();
logFile.close();
targetSegment = segments.get(nextSegmentIndex);
RandomAccessFile nextLogFile = new RandomAccessFile(...);   // opened…
FileChannel nextLogChannel = nextLogFile.getChannel();      // …but never used
nextLogChannel.position(0);
```

The loop keeps using the **old, closed** `logChannel`. The next `logChannel.position()` throws
`ClosedChannelException`, which is caught as an `IOException`. The method then returns what it read so far, and the
new file is **never closed** (a file-handle leak).

Test with segments `[0..13]` and `[14..]`:
```
readMessages(3, 10 MB)  → 11 messages (3..13) + SEVERE ClosedChannelException
readMessages(13, 10 MB) → 1 message  (13)     + SEVERE ClosedChannelException
```
The consumer still gets correct data. It just needs one extra fetch per segment boundary, and the logs fill with
errors. A correct version would switch `logFile`/`logChannel` to the next segment's file (they're
try-with-resources variables, so the code would need restructuring), or simply stop at the end of the segment.
Real Kafka also returns data from **one segment per fetch per partition**.

### Problem 2: a message bigger than `maxBytes` blocks the consumer (verified)

```java
if (bytesRead + messageSize > maxBytes) break;
```
If the **first** message is bigger than `maxBytes`, the result is empty. The consumer asks again with the same offset
and gets nothing again, forever:
```
100 KB message, maxBytes = 1000 → 0 messages
```
Real Kafka fixed exactly this in KIP-74: a fetch always returns **at least the first batch**, even if it's
bigger than `fetch.max.bytes` / `max.partition.fetch.bytes`.

Also note the check ignores the 4-byte size header: `maxBytes = 5` returns "hello" (9 bytes on disk).

### In real Kafka

`ReplicaManager.fetchMessages` → `UnifiedLog.read(startOffset, maxLength, isolation, minOneMessage)`:
1. **Upper limit** depends on who's asking:
   - follower → up to **LEO** (it must copy everything)
   - consumer with `isolation.level=read_uncommitted` → up to the **high watermark**
   - consumer with `read_committed` → up to the **last stable offset** (no open transactions)
2. `offset < log start offset` or `> LEO` → `OFFSET_OUT_OF_RANGE` error. `offset ≥ limit` → empty response (the
   request may wait up to `fetch.max.wait.ms` for data, the "long poll").
3. Find the segment (`floorEntry`), then `LogSegment.read()`: index lookup + short scan to the position of the batch
   containing `startOffset`.
4. **Nothing is read into the JVM.** The result is a `FileRecords` **slice**: (file, start position, length).
   Later the network layer calls `FileChannel.transferTo()`, and the OS copies page cache → socket directly
   (**zero-copy**, `sendfile`). With TLS the bytes must be encrypted, so they are copied through the JVM.
5. Whole batches only. The consumer drops the records before `startOffset` itself.
6. `minOneMessage = true` for the first partition in the response: the first batch is always returned, even if it's
   bigger than `max.partition.fetch.bytes` (the fix for Problem 2).
7. Data from **one segment** only. If the segment ends, the next fetch continues in the next segment.

---

## `findSegmentForOffset(offset)`

### Purpose

Finds the segment containing `offset` with a **binary search** on base offsets. Returns `null` if
`offset >= nextOffset` (the message doesn't exist yet).

Segment `i` holds the offsets `[base(i), base(i+1))`, and the last segment holds `[base(last), nextOffset)`.

### Example: segments `[0, 14, 30, 47]`, find offset 35

```
low=0 high=3 → mid=1 (base 14, next 30):  35 in [14,30)? no. 35 ≥ 14 → low=2
low=2 high=3 → mid=2 (base 30, next 47):  35 in [30,47)? yes → segment 30
```

With 1000 segments that's about 10 steps instead of 1000.

### In real Kafka

Segments are kept in a `ConcurrentSkipListMap<Long, LogSegment>` keyed by base offset, and the lookup is one call:
`segments.floorEntry(offset)`, which returns the largest base offset ≤ `offset`. It's the same idea as the binary search,
but the map is also safe for readers while the writer adds or deletes segments, so readers need no lock.

---

## `findPositionForOffset(segment, offset)`

### Purpose

Turns an offset into a byte position inside the segment's `.log` using the `.index`.

```java
long relativeOffset = offset - segment.getBaseOffset();   // 35 - 30 = 5 → 6th message in this segment
indexChannel.position(relativeOffset * 16);               // entry 5 starts at byte 80
buffer.getLong();          // skip the stored offset (we already know it)
return buffer.getLong();   // the position
```

Because the index is dense, this is a direct jump with no search.

### Problem: wrong fallback when the index is missing an entry

```java
if (relativeOffset >= entryCount) {
    // returns the position of the LAST indexed message
}
```

When can the index have fewer entries than the log? When a crash happens after `activeLogChannel.write()` but before
`updateIndex()`. Then for the missing offset this returns the position of an **earlier** message. `readMessages` would
read that earlier message, and the broker would label it with the wrong offset, so the consumer gets **duplicate
data with wrong offsets**. A safer fallback is to scan forward from the last indexed position, counting records.

There is also `if (indexChannel.size() == 0) return 0;`. For an empty index (base offset with no entries), it
starts at the beginning of the `.log`. That's correct only if `offset == baseOffset`.

### In real Kafka

Two steps, `OffsetIndex.lookup()` then `FileRecords.searchForOffsetWithSize()`:

```
segment 00000000000000042000, target offset 42080 → relative 80
.index (relative offset, position):  (0, 0) (37, 4105) (75, 8230) (112, 12290) …
1. binary search: largest entry ≤ 80 → (75, 8230)
2. scan the .log from byte 8230, reading only the 12-byte start of each batch header (base offset + length):
     batch at 8230: offsets 42075..42079 → last < 42080, jump forward by its length
     batch at 8410: offsets 42080..42091 → contains 42080 ✔ → return position 8410
```

The scan is never longer than about 4 KB (`log.index.interval.bytes`), because there's an index entry at least every 4 KB.
An empty index isn't a problem: the lookup returns position 0 and the scan starts from the beginning of the segment.

Kafka's binary search first looks in the **last few KB** of the index ("warm section"), because most fetches ask
for recent offsets, and those index pages are surely in memory.

---

## Getters and setters

| Method | Who calls it | Why |
|---|---|---|
| `getId()` | broker | find the partition in a topic's list (`p.getId() == partition`) |
| `getLeader()` | broker | "am I the leader?" in produce. If not, forward to the leader |
| `setLeader(id)` | controller's `rebalancePartitions()` | the old leader died, assign a new one |
| `getFollowers()` | broker's `replicateToFollowers()` | who to send the copy to. Returns a **copy**, so callers can't change the internal list |
| `setFollowers(list)` | `rebalancePartitions()` | new replica set |
| `getLogEndOffset()` | broker's `handleFetchRequest()` | `offset >= LEO` → reply "0 messages" without touching the disk |

Note: `leader`/`followers` are written by the ZooKeeper event thread (rebalance) and read by request threads, with no lock
and no `volatile`. In theory, one thread may not see the new value right away. This is a small gap, not something to
worry about yet.

### In real Kafka

Leadership lives in `kafka.cluster.Partition`, not in the log. A broker never decides it by itself. It gets it
from the controller's metadata as `PartitionRecord` / `PartitionChangeRecord` in KRaft:
`leader`, **`leaderEpoch`**, `replicas`, `isr`. Then `makeLeader()` or `makeFollower()` runs:
- **makeLeader**: start accepting produce requests, start tracking each follower's fetch offset, and compute the HW.
- **makeFollower**: stop accepting writes, truncate the log if needed (leader epoch), and start a fetcher
  thread that fetches from the new leader.

The **leader epoch** goes up by one with every leader change. It's the fencing number for partitions,
the same idea as the controller epoch in Q39. Requests that carry an old epoch are rejected (`FENCED_LEADER_EPOCH`).

The log has several offsets, not just one:
`logStartOffset`, `highWatermark`, `lastStableOffset`, `logEndOffset`. Fetch uses the one that matches the client
(see `readMessages` above).

## `close()`

Takes the **write lock** (so no append is half-done), then closes the active channel and file. Called on broker shutdown.
After `close()`, any `append` fails.

### In real Kafka

A controlled shutdown first asks the controller to **move leadership away** from this broker
(`controlled.shutdown.enable=true`), so producers switch to other leaders. Then `LogManager.shutdown()` runs:
1. flushes every log (fsync)
2. writes `recovery-point-offset-checkpoint` and the other checkpoint files
3. writes the **`.kafka_cleanshutdown`** marker. On the next start this marker means "skip recovery",
   which makes the restart fast.

With `kill -9` none of this happens, so the next start does full recovery. With many partitions that can
take minutes.

## `SegmentInfo`

A small immutable holder: `baseOffset`, `logPath`, `indexPath`. It's `private static`. Nobody outside `Partition` needs to
know about segments. The broker only thinks in offsets.

---

## Concurrency: why `ReadWriteLock`

```
Thread A: fetch from offset 0   ─┐ read lock  ─ can run together
Thread B: fetch from offset 50  ─┘
Thread C: append                ─── write lock ─ waits until A and B finish, then blocks new readers
```

Readers don't change anything, so letting them run in parallel is safe. The writer changes `segments`, the files and
`nextOffset`, so it must run alone.

### In real Kafka

- **Appends** are serialized by a lock on the `UnifiedLog`, one writer per partition. Different partitions are
  written in parallel, which is another reason partitions give throughput.
- **Reads take no lock at all.** Written bytes never change, and a reader first takes a snapshot of the HW/LEO
  (with its file position) and never reads past it. So it can't see a half-written batch, even while the writer
  is appending.
- Segment deletion by retention is safe for readers: the file is first renamed to `.deleted`
  and actually removed only after `file.delete.delay.ms` (default 60 s), when no reader is still using it.

---

## Verified problems summary

| # | Where | What happens | Real Kafka |
|---|---|---|---|
| 1 | `readMessages` segment switch | `ClosedChannelException`, read stops at the segment end, the next file is leaked | one segment per fetch, by design |
| 2 | `readMessages` max-bytes check | message > `maxBytes` → consumer stuck forever | always returns ≥ 1 batch (KIP-74) |
| 3 | `countMessagesInSegment` | half-written record counted → offset stuck forever | recovery: CRC check + truncate |
| 4 | `findPositionForOffset` fallback | missing index entry → wrong message returned | index rebuilt on recovery; sparse index + scan |
| 5 | `append` | 2 fsyncs + index file open per message → slow | page cache, flush by the OS, durability via replication |
| 6 | `initialize` | errors swallowed → NPE on the first append | broker marks the log dir offline |
| 7 | `append` on followers | follower assigns its own offsets → offsets diverge after one lost message | `appendAsFollower` keeps the leader's offsets |

These are the reference code's behaviour, not mistakes you made when copying. They're worth knowing, and #1 and #2
are good candidates to fix later.

---

## Mapping to real Kafka configs

| simple-kafka | Real Kafka config | Default |
|---|---|---|
| `data/<brokerId>/` | `log.dirs` | `/tmp/kafka-logs` |
| `<topic>/<partition>/` | directory `<topic>-<partition>`, e.g. `orders-0` | |
| `DEFAULT_SEGMENT_SIZE` (1 MB) | `log.segment.bytes` / topic `segment.bytes` | 1 GB |
| — | `log.roll.ms` / `segment.ms` (roll by time too) | 7 days |
| one index entry per message | `log.index.interval.bytes` | 4096 |
| `force(true)` per message | `log.flush.interval.messages` / `.ms` | not set: the OS decides |
| `maxBytes` in `readMessages` | `max.partition.fetch.bytes` / `fetch.max.bytes` (consumer) | 1 MB / 50 MB |
| — (never deletes) | `log.retention.hours` / `retention.bytes` / `cleanup.policy` | 168 h / -1 / delete |

# Brokers, Leaders and Replicas (how real Kafka works)

General concepts about brokers, described the way **real Kafka** works. Method explanations live in the
`stage-N-*.md` files. Related: `partition.md` (storage, replication details), `protocol.md` (requests),
`kraft.md` (controllers).
Quiz: `quiz-2-cluster-and-zookeeper.md`, `quiz-4-cluster-internals.md`

Contents:
1. What a broker is · 2. Node roles in KRaft · 3. Inside a broker: threads and components ·
4. The life of a request · 5. Purgatory: requests that wait · 6. Listeners and advertised listeners ·
7. Partition leaders and replicas · 8. Replication on the broker · 9. Leader election: all the cases ·
10. Coordinators: special jobs of some brokers · 11. Broker lifecycle · 12. Which broker does a client talk to? ·
13. Rack awareness · 14. Important broker configs · 15. Metrics to watch · 16. What can cause a broker to "die"? ·
17. Does one dead pod kill all brokers? · 18. simple-kafka vs real Kafka · 19. Try it

---

## 1. What is a broker?

A **broker is one Kafka server**: a running program (a JVM process) on a machine, listening on a
TCP port (usually 9092). It:
1. **receives** records from producers (Produce requests) and **writes them to disk**
2. **sends** stored records to consumers and to follower brokers (Fetch requests)
3. **answers questions** about the cluster (Metadata requests)
4. **coordinates** consumer groups and transactions (if it's their coordinator, section 10)

Producers and consumers never talk to each other. They only talk to brokers.
The broker is the "post office" in the middle.

```
Producer ──Produce──▶  BROKER  ──Fetch──▶ Consumer
                      (stores records on disk)
```

The producer and consumer don't need to run at the same time: the broker keeps the records
until retention deletes them, and consumers read them whenever they want.

Several brokers working together = a **Kafka cluster**. Partitions are spread across them, so
the load is shared and a copy survives if one broker dies.

A broker is **"dumb" by design**: it doesn't track which consumer read which record, doesn't route records by content,
and doesn't change records (it stores batches as they arrive). That simplicity is part of why it's fast.

In simple-kafka the broker is `SimpleKafkaBroker.java`: it opens a socket, reads the bytes produced by
our `encode...Request` methods, decodes them, and writes/reads messages in `Partition.java`.
`BrokerInfo` (id, host, port) is how one broker is described to clients.

---

## 2. Node roles in KRaft

Since Kafka 4.0 every cluster runs in KRaft mode, and each node has `process.roles`:

| Role | Does |
|---|---|
| `broker` | stores partitions, handles clients: what this file is about |
| `controller` | one of the 3 (or 5) controllers that manage cluster metadata (`kraft.md`) |
| `broker,controller` | both (dev and small clusters only) |

So "the controller" is **not** a broker any more. In ZooKeeper mode it was one of the brokers with an extra job.

---

## 3. Inside a broker: threads and components

```
                          clients / other brokers
                                   │ TCP
┌──────────────────────────────────▼────────────────────────────────────────────────────────┐
│ SocketServer (one per listener)                                                           │
│   Acceptor thread ── accepts connections, hands each to a Processor                       │
│   Processor threads (num.network.threads = 3): read bytes → whole requests, write responses│
└──────────────┬───────────────────────────────────────────────▲────────────────────────────┘
               │ RequestChannel: request queue (queued.max.requests = 500)  │ response queues
┌──────────────▼───────────────────────────────────────────────┴────────────────────────────┐
│ Request handler threads (num.io.threads = 8) → KafkaApis: "which API? call the right part" │
└──┬──────────────┬──────────────────┬──────────────────┬────────────────┬──────────────────┘
   ▼              ▼                  ▼                  ▼                ▼
ReplicaManager  GroupCoordinator   TransactionCoord.  MetadataCache    AutoTopicCreation / Admin
 ├ Partition (per partition: leader?, ISR, HW)          (applied __cluster_metadata)  → forwarded to controller
 │  └ UnifiedLog → LogSegments → files
 ├ ReplicaFetcherManager (num.replica.fetchers = 1 thread per source broker): followers fetching from leaders
 └ Purgatories: produce (acks=all), fetch (long poll)
LogManager: all logs on all log.dirs, retention, flush, recovery, cleaner threads (compaction)
BrokerLifecycleManager: registration + heartbeats with the controller
```

- **Network threads** only move bytes. **I/O threads** do the work (append to the log, read, coordinator logic).
  If the I/O threads are busy, requests wait in the queue. That's the `RequestQueueTimeMs` metric.
- When the request queue is full (500), network threads stop reading new requests (**back-pressure**).
- A broker is mostly **I/O** (disk, network) and needs little CPU, unless it uses TLS or recompresses.

---

## 4. The life of a request

A Produce with `acks=all`, and where its time goes (these are the broker's request metrics):

```
1. Processor reads the size + bytes from the socket            ┐
   → one complete request → request queue                       │ RequestQueueTimeMs: waiting for an I/O thread
2. I/O thread takes it → KafkaApis.handleProduceRequest         ┘
   → auth check (ACLs) → ReplicaManager.appendRecords           ┐ LocalTimeMs: append to the leader's log
   → Partition.appendRecordsToLeader → UnifiedLog.appendAsLeader┘   (page cache)
3. acks=all → put in the produce purgatory                      ─ RemoteTimeMs: waiting for followers to fetch
   followers fetch → HW passes the batch → request completes
4. response → response queue → Processor writes it              ─ ResponseQueueTimeMs + ResponseSendTimeMs
                                                                 = TotalTimeMs
```

When produce latency is high at work, these metrics show **where** it is: queue time → not enough I/O threads,
local time → slow disk, remote time → slow followers or network.

While a request from one connection is being processed, the broker **mutes** that connection (stops reading more
requests from it). That's how responses stay in order per connection (`protocol.md`, section 10).

---

## 5. Purgatory: requests that wait

Some requests can't be answered yet, and a thread can't block on them. They're parked in a **purgatory**: a timer
wheel with "check again when X happens".

| Purgatory | Request waits until | Or times out after |
|---|---|---|
| produce | HW ≥ the batch's last offset (`acks=all`) | the request's `timeout_ms` (`request.timeout.ms` 30 s on the producer) |
| fetch | ≥ `fetch.min.bytes` available | `fetch.max.wait.ms` (500 ms): the "long poll" |
| join group, heartbeat … | the rebalance finishes | session timeout etc. |

This is why a consumer with no new data doesn't spin: its Fetch sits in purgatory for up to 500 ms, and it's
answered the moment data arrives.

---

## 6. Listeners and advertised listeners

The most common Kafka setup problem, especially in Docker and Kubernetes.

- **`listeners`**: where the broker **binds** (which interface and port it listens on).
- **`advertised.listeners`**: the address the broker **tells clients** to use, in Metadata responses.

```properties
listeners=INTERNAL://0.0.0.0:9092,EXTERNAL://0.0.0.0:9094,CONTROLLER://0.0.0.0:9093
advertised.listeners=INTERNAL://kafka-0.kafka-headless.svc:9092,EXTERNAL://kafka-0.example.com:9094
listener.security.protocol.map=INTERNAL:PLAINTEXT,EXTERNAL:SASL_SSL,CONTROLLER:PLAINTEXT
inter.broker.listener.name=INTERNAL        # which listener brokers use to replicate from each other
controller.listener.names=CONTROLLER
```

**The classic failure:**
```
1. app → bootstrap kafka.example.com:9094 → connected ✔
2. Metadata response: "leader of orders-0 is broker 0 at kafka-0.kafka-headless.svc:9092"
3. app (outside Kubernetes) → kafka-0.kafka-headless.svc:9092 → can't resolve / timeout ✘
```
Bootstrap works, but produce and fetch fail. The fix: each listener advertises an address that **the clients of that
listener** can reach. That's why there are separate internal and external listeners.

---

## 7. Partition leaders and replicas

### Replicas
Each partition is stored on **several brokers** so the data survives if one dies.
How many copies = **replication factor** (set when the topic is created, `default.replication.factor`).

```
topic "orders", 3 partitions, replication factor 2, 3 brokers

            broker 1     broker 2     broker 3
orders-0    LEADER       follower
orders-1                 LEADER       follower
orders-2    follower                  LEADER
```

Every partition has 2 copies. **One copy is the leader, the others are followers.**

### Leadership is per partition, not per broker
There is no single "leader broker". Every broker is the leader for **some** partitions and a follower
for **others**, at the same time. Above, broker 2 leads `orders-1` and follows `orders-0`.
So any broker that dies was the leader of some partitions, and only **those** partitions get a new leader.

(The one cluster-wide special role is the **active controller**: it decides who leads what.
It's a different thing from partition leadership. In KRaft it's a separate node, see `kraft.md`.)

### Leader vs follower
- **Leader**: the only copy that producers write to and (normally) consumers read from.
  It decides the order of records and gives them their offsets.
- **Follower**: just keeps a copy. It **fetches from the leader**, exactly like a consumer does
  (it sends Fetch requests), and appends what it gets to its own log **with the leader's offsets**.

Why only one leader? If two brokers accepted writes for the same partition, they could give
offset 5 to two different records. One leader = one order, one truth.

Leaders are **spread across brokers** (see the table) so every broker does some of the work. The first replica in
the list is the **preferred leader**, and the controller moves leadership back to it after failures
(`auto.leader.rebalance.enable`).

### Example: a record is written
`producer.send("orders", partition 1, "hi")`:
1. metadata says leader of orders-1 = broker 2 → producer sends to broker 2
2. broker 2 appends "hi" at offset 7 (LEO 8)
3. broker 3 (follower) fetches from broker 2: Fetch(offset 7) → gets "hi" at offset 7
4. broker 3's next Fetch(offset 8) tells broker 2 "I have up to 8" → HW = 8 → committed
5. `acks=all` producer gets its OK now

### In-sync replicas (ISR)
Followers that are caught up with the leader are **in sync**. The ISR list = leader + caught-up followers.
A follower that hasn't caught up to the leader's end for longer than `replica.lag.time.max.ms` (default 30 s) is removed
from the ISR. The leader asks the controller to change the ISR with `AlterPartition`.

### Example: a broker dies (failover)
```
💥 broker 2 dies
  orders-0 → leader still broker 1 (only lost a follower, nothing changes for clients)
  orders-1 → leader moves to broker 3, ISR [2, 3] → [3]
  orders-2 → leader still broker 3
```
1. the controller notices broker 2 is gone (no heartbeat for 9 s → fenced)
2. for each partition broker 2 led, it picks a new leader **from the ISR** (it has all committed data)
   and increments the **leader epoch**
3. clients sending to broker 2 get errors → refresh metadata → learn the new leader → continue
4. when broker 2 comes back, it becomes a **follower**, truncates any uncommitted records (leader epoch),
   catches up, rejoins the ISR, and later gets its leadership back (preferred leader election)

Partitions led by other brokers keep working without a pause. Clients of `orders-1` see a short
pause and some retries.

### Why `acks` matters here (producer config)
- `acks=0`: don't wait at all. Fastest, can lose records.
- `acks=1`: leader wrote it. If the leader dies before followers copied it → **record lost**.
- `acks=all`: all ISR replicas have it. Survives a leader crash. (Default since Kafka 3.0.)

`acks=all` + topic `min.insync.replicas=2` = "only accept a write if at least 2 copies have it".
If the ISR shrinks below 2, producers get `NotEnoughReplicasException` instead of silently
writing to just one copy. Typical safe setup: replication factor 3, `min.insync.replicas=2`, `acks=all`.

### Gotcha: unclean leader election
If **all** ISR replicas are dead, Kafka can either wait for one to come back (default,
`unclean.leader.election.enable=false`), or pick an out-of-sync follower as leader → records it
didn't have yet are **lost**. Default chooses consistency over availability.

See it on a real cluster: `kafka-topics.sh --describe --topic <name>` shows the leader, replicas and ISR
of every partition.

---

## 8. Replication on the broker

Every broker that follows some partitions runs **replica fetcher threads**, one per leader broker it follows
(`num.replica.fetchers` = 1 → one thread per source broker). Each thread sends **one Fetch for all partitions** it
follows on that leader:

```
broker 3's fetcher for leader broker 2:
  Fetch(orders-1 @ 512, payments-4 @ 9031, …)  → replica_id = 3
  ← batches for each partition + the leader's HW
  → appendAsFollower for each (same offsets), update local HW = min(leader HW, my LEO)
  → repeat (the request waits in the leader's fetch purgatory if there's nothing new)
```

- Replication is **pull**, not push. The leader doesn't need to know how fast each follower is, and a slow follower
  doesn't slow the leader's appends. It only delays the HW (and `acks=all`), until it drops out of the ISR.
- `replica.fetch.max.bytes` (1 MB per partition), `replica.fetch.wait.max.ms` (500 ms).
- Replication throughput can be limited during reassignments (`leader.replication.throttled.rate`).

simple-kafka does the opposite: the leader **pushes** each message to followers with a `REPLICATE` request
(`stage-2-protocol.md`, "Push vs pull").

---

## 9. Leader election: all the cases

| Case | Who becomes leader | Data loss? |
|---|---|---|
| **Leader broker fails** | first live replica in the **ISR** (in replica-list order) | no (committed data is on all ISR) |
| **Controlled shutdown** | the controller moves leadership to another ISR member **before** the broker stops | no, and clients barely notice |
| **Preferred leader election** | moves leadership back to the first replica in the list, if it's in the ISR. Automatic every `leader.imbalance.check.interval.seconds` (300) when a broker's imbalance > `leader.imbalance.per.broker.percentage` (10%), or manual with `kafka-leader-election.sh` | no |
| **Reassignment** | a replica in the new list, after it caught up | no |
| **Unclean election** | any live replica, even out of sync (only if `unclean.leader.election.enable=true`) | **yes** |

Every election increments the partition's **leader epoch**. Produce and Fetch requests carry the epoch the client
knows, and a broker with a different epoch answers `FENCED_LEADER_EPOCH` / `UNKNOWN_LEADER_EPOCH`, so nobody keeps
writing to an old leader.

---

## 10. Coordinators: special jobs of some brokers

Some brokers have extra jobs for some clients. They're chosen by **partition leadership of an internal topic**:

| Coordinator | For | Lives on | Chosen by |
|---|---|---|---|
| **Group coordinator** | one consumer group: members, rebalances, committed offsets | leader of a partition of `__consumer_offsets` (50 partitions) | `abs(hash(group.id)) % 50` |
| **Transaction coordinator** | one transactional producer | leader of a partition of `__transaction_state` (50) | `abs(hash(transactional.id)) % 50` |

```
group.id = "billing-service" → hash % 50 = 17 → leader of __consumer_offsets-17 is broker 2
→ broker 2 is the group coordinator of "billing-service"
```

The client finds it with a `FindCoordinator` request. If broker 2 dies, leadership of `__consumer_offsets-17` moves, so
the coordinator moves with it, and its state is reloaded from that partition. **Coordinators are stateless apart from their
log.** The same pattern again.

---

## 11. Broker lifecycle

```
start
 ├─ load logs from log.dirs (recovery if the last shutdown was unclean: partition.md section 17)
 ├─ register with the controller (BrokerRegistration) → new broker epoch → FENCED
 ├─ catch up on __cluster_metadata
 ├─ heartbeat "caught up" → UNFENCED → becomes leader/follower of its partitions, starts fetchers
 │      ... heartbeats every 2 s ...
 ├─ SIGTERM → controlled shutdown: controller moves leaders away → broker fenced → stop fetchers
 ├─ flush logs, write checkpoints + .kafka_cleanshutdown
 └─ exit
```

A **rolling restart** at work = restart brokers one by one, each time waiting until `UnderReplicatedPartitions` is 0
again before the next one. If you restart the next broker too early, some partitions may have only one in-sync copy,
and with `min.insync.replicas=2` they reject `acks=all` writes.

---

## 12. Which broker does a client talk to?

Produce and fetch go to the **leader of that partition**. A consumer reading 3 partitions led by
3 different brokers has connections to **all 3 brokers**.

How the client finds the leader:
1. `bootstrap.servers` gives it a few brokers to start with (any of them, just to get in)
2. it sends a **Metadata request** → learns all brokers (id, host, port from `advertised.listeners`) and the leader of each partition
3. it connects directly to the right leader for each partition
4. if the leader dies → error `NOT_LEADER_OR_FOLLOWER` → refresh metadata → go to the new leader

Different brokers can have different jobs for the same consumer:
- **partition leader** → where it fetches records
- **group coordinator** → where it joins the group and commits offsets (`__consumer_offsets`)

(Since Kafka 2.4, with `client.rack` set and the broker's `replica.selector.class` configured, a consumer can fetch from a follower in its own zone
to save cross-zone traffic.)

---

## 13. Rack awareness

Set `broker.rack=eu-1a` (zone, rack, data centre) on each broker. When a topic is created, the controller places each
partition's replicas in **different racks**:

```
3 zones, RF 3:  orders-0 → [broker 1 (eu-1a), broker 4 (eu-1b), broker 7 (eu-1c)]
```

A whole zone can fail, and every partition still has 2 copies. Without `broker.rack`, all 3 replicas could land in the
same zone.

---

## 14. Important broker configs

| Config | Default | What |
|---|---|---|
| `node.id`, `process.roles` | — | identity and role (KRaft) |
| `listeners`, `advertised.listeners` | — | section 6 |
| `log.dirs` | `/tmp/kafka-logs` | where partitions live. **Change it**: /tmp may be wiped on reboot |
| `num.partitions` | 1 | partitions of auto-created topics |
| `default.replication.factor` | 1 | RF of auto-created topics. Production: 3 |
| `min.insync.replicas` | 1 | production: 2 (with RF 3) |
| `auto.create.topics.enable` | true | often disabled in production (typos create topics) |
| `unclean.leader.election.enable` | false | |
| `log.retention.hours`, `log.segment.bytes` | 168, 1 GB | `partition.md` |
| `num.network.threads`, `num.io.threads` | 3, 8 | section 3 |
| `num.replica.fetchers` | 1 | |
| `replica.lag.time.max.ms` | 30000 | ISR |
| `message.max.bytes` | ~1 MB | biggest batch (topic: `max.message.bytes`) |
| `broker.rack` | — | section 13 |
| `offsets.topic.replication.factor` | 3 | RF of `__consumer_offsets` |

Most can be overridden **per topic** (`retention.ms`, `min.insync.replicas`, …) and many can be changed **without a
restart** (dynamic configs, `kafka-configs.sh --alter --entity-type brokers`).

---

## 15. Metrics to watch

| Metric | Healthy | Meaning when not |
|---|---|---|
| `UnderReplicatedPartitions` | 0 | a follower is behind or a broker is down (first warning) |
| `UnderMinIsrPartitionCount` | 0 | ISR < `min.insync.replicas`: `acks=all` writes are **failing** |
| `OfflinePartitionsCount` | 0 | a partition has **no** leader, nobody can read or write it (serious) |
| `ActiveControllerCount` (on controllers) | exactly 1 in total | 0 = no controller, >1 = split brain |
| `IsrShrinksPerSec` / `IsrExpandsPerSec` | ~0 | flapping followers |
| `RequestHandlerAvgIdlePercent` | > 30% | I/O threads saturated |
| `NetworkProcessorAvgIdlePercent` | > 30% | network threads saturated |
| `TotalTimeMs` (Produce / FetchConsumer) | stable | split by queue / local / remote time (section 4) |
| disk usage, GC pause time | | |

---

## 16. What can cause a broker to "die"?

"Dead" for the cluster means **the controller stopped hearing from the broker** (missed heartbeats
longer than `broker.session.timeout.ms`, default 9 s in KRaft mode). The process doesn't have to crash.

**1. The process crashes**
- **Disk full**: the most common. Retention too long, a traffic spike, no `retention.bytes` limit.
- **Too many open files**: every segment `.log`/`.index`/`.timeindex` file and every client socket is an
  open file. Many partitions + low `ulimit -n` → crash.
- **Out of memory**: Java heap (`OutOfMemoryError`), or `Map failed` from too many memory-mapped index
  files (Linux `vm.max_map_count`).
- **Disk hardware / I/O errors.** With several `log.dirs`, a failed disk takes only its partitions offline,
  and the broker keeps running with the others.

**2. The machine goes away**
- Hardware failure, power, VM/host reboot, cloud instance retired.
- **Kubernetes/Docker OOMKilled**: the container exceeds its memory limit. Remember that the page cache counts
  towards the container's memory in some setups. Give the JVM a small heap and leave the rest for the OS.

**3. It's alive but *looks* dead**
- **Long GC pause** → misses heartbeats → controller moves its leaders away.
- **Network problem** between the broker and the controller.
- **Overloaded** (CPU, slow disk) → falls out of ISR, may miss heartbeats.
- When it repeats it's **flapping**: leaders keep moving back and forth.

**4. Planned (the most frequent in practice)**
- Rolling restarts for upgrades/config changes, pod rescheduling.
- With **controlled shutdown** (default) the broker hands its leaders to other ISR members *before*
  stopping, so clients just follow the new leader.

---

## 17. Does one dead container/pod kill all brokers?

No. Normally it's **one broker per container/pod**, each on a different machine:

```
Kubernetes:
  node A → pod kafka-0 (broker 1)
  node B → pod kafka-1 (broker 2)
  node C → pod kafka-2 (broker 3)
```

Pod `kafka-1` OOMKilled → only broker 2 dies → brokers 1 and 3 take over its partitions →
Kubernetes restarts the pod → broker 2 comes back as a follower. With a **persistent volume** it keeps its data and
only copies what it missed. Without one it starts empty and must copy **all** its partitions from the leaders again.

**All brokers die only if they share what failed:**
- all on **one machine** (e.g. `docker-compose` on a laptop or one server)
- all pods on the **same Kubernetes node** → that's why deployments use *anti-affinity* rules
- all nodes in **one data center / availability zone** → that's why production spreads brokers
  across zones and sets `broker.rack`, so Kafka puts each partition's replicas in different zones

Replication only protects you if **the copies live in places that don't fail together**.
Replication factor 3 on one machine protects you from nothing.

---

## 18. simple-kafka vs real Kafka

| | simple-kafka | real Kafka |
|---|---|---|
| Threads | one fixed pool of 10 threads shared by the accept loop, every client connection and replication. The 10th open connection waits for a free thread | acceptor + network threads + I/O thread pool + fetchers; thousands of connections on a few threads (NIO selector) |
| Waiting requests | none: answers immediately | purgatories (`acks=all`, long poll) |
| Replication | leader **pushes** each message | followers **pull** with Fetch |
| Follower offsets | follower assigns its own | follower keeps the leader's |
| ISR / HW | none | yes |
| Leader election | first broker in the list | from the ISR, with leader epoch |
| Controller | a broker, elected via ZooKeeper | separate controller quorum (KRaft) |
| Coordinators | none (consumer keeps its own offset) | group + transaction coordinators |
| Listeners | one host:port | several listeners, advertised addresses, TLS/SASL |
| Rack awareness | none | `broker.rack` |

---

## 19. Try it

```bash
# all brokers and their supported API versions
kafka-broker-api-versions.sh --bootstrap-server <broker>:9092 | grep -E '^[^ ]'

# partitions that are under-replicated / without a leader / below min ISR
kafka-topics.sh --bootstrap-server <broker>:9092 --describe --under-replicated-partitions
kafka-topics.sh --bootstrap-server <broker>:9092 --describe --unavailable-partitions
kafka-topics.sh --bootstrap-server <broker>:9092 --describe --under-min-isr-partitions

# a broker's effective config (including defaults)
kafka-configs.sh --bootstrap-server <broker>:9092 --describe --entity-type brokers --entity-name 1 --all

# move leaders back to preferred replicas
kafka-leader-election.sh --bootstrap-server <broker>:9092 --election-type PREFERRED --all-topic-partitions
```

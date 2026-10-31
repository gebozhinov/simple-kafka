# KRaft (how real Kafka coordinates without ZooKeeper)

General concepts. KRaft is how Kafka manages cluster metadata today: production-ready since Kafka 3.3, and the
**only** mode since Kafka 4.0, which removed ZooKeeper. ZooKeeper basics and the ZooKeeper-era design: `zookeeper.md`.
Quiz: `quiz-4-cluster-internals.md`

Contents:
1. What KRaft is · 2. Why it replaced ZooKeeper · 3. Roles: controllers and brokers · 4. Configuration ·
5. The metadata log (`__cluster_metadata`) · 6. Raft: how the controllers agree · 7. Leader election example ·
8. Brokers: registration, heartbeats, fencing · 9. What the active controller does · 10. Example: a broker dies ·
11. Example: a topic is created · 12. Snapshots · 13. Files on disk · 14. Failure cases ·
15. ZooKeeper mode vs KRaft · 16. simple-kafka vs KRaft · 17. Try it

---

## 1. What KRaft is

**KRaft = Kafka + Raft.** A few Kafka nodes act as **controllers**. They keep all cluster metadata in **one
replicated log**, `__cluster_metadata`, and agree on it with the **Raft** consensus algorithm. One of them is the
**active controller** (the Raft leader). It's the only one that changes metadata. Everyone else, including all
brokers, **reads the log** and applies the changes in order.

```
                    ┌──────────── controller quorum ───────────┐
                    │ controller 1   controller 2   controller 3│
                    │  (follower)     (LEADER =      (follower) │
                    │                  active)                  │
                    └───────────────────▲───────────────────────┘
                     Fetch __cluster_metadata (pull, like followers fetch a partition)
           ┌────────────────┬───────────┴────┬────────────────┐
         broker 4        broker 5        broker 6         broker 7
     (metadata cache) (metadata cache) (metadata cache) (metadata cache)
```

**Metadata** = everything about the cluster that isn't messages: which brokers exist, topics and their ids, partitions,
the leader, leader epoch, replicas and ISR of every partition, configs, ACLs, quotas, producer id blocks, and feature
versions.

The idea in one sentence: **Kafka uses its own best idea, the replicated log, for its own metadata.**

---

## 2. Why it replaced ZooKeeper

(This is Q40 in quiz 2, in more detail.)

| Problem with ZooKeeper | How KRaft solves it |
|---|---|
| **Two systems** to deploy, secure, monitor and upgrade | one system, one config style, one security model |
| **Slow controller failover**: the new controller had to read the full state from ZooKeeper (minutes in big clusters) | standby controllers already have the whole log in memory → failover in about a second |
| **Limited scale**: around 200k partitions per cluster | millions of partitions |
| **Pushing metadata** to brokers with RPCs (`UpdateMetadata`, `LeaderAndIsr`); a broker that missed one was silently out of date | brokers **pull** an ordered log and know their exact position (offset) |
| **Two sources of truth**: ZooKeeper and the controller's memory could disagree | the log is the only truth; memory is just the log applied |

---

## 3. Roles: controllers and brokers

Each Kafka node has `process.roles`:

| `process.roles` | What the node does | Use |
|---|---|---|
| `broker` | stores partitions, serves clients | production |
| `controller` | member of the controller quorum, manages metadata | production: 3 (or 5) dedicated controllers |
| `broker,controller` | both in one JVM ("combined mode") | dev, small clusters |

- The **quorum** has 3 or 5 controllers (a majority must be alive: 3 → can lose 1, 5 → can lose 2, same as ZooKeeper).
- **Clients never talk to controllers.** Admin requests (CreateTopics, …) go to any broker, which **forwards** them
  to the active controller.
- Every node has a unique `node.id`. Brokers and controllers share the same id space.

---

## 4. Configuration

```properties
# a dedicated controller
process.roles=controller
node.id=1
controller.quorum.bootstrap.servers=ctrl-1:9093,ctrl-2:9093,ctrl-3:9093   # 3.9+; older: controller.quorum.voters=1@ctrl-1:9093,2@ctrl-2:9093,3@ctrl-3:9093
controller.listener.names=CONTROLLER
listeners=CONTROLLER://:9093
log.dirs=/var/lib/kafka/meta

# a broker
process.roles=broker
node.id=4
controller.quorum.bootstrap.servers=ctrl-1:9093,ctrl-2:9093,ctrl-3:9093
controller.listener.names=CONTROLLER
listeners=PLAINTEXT://:9092
advertised.listeners=PLAINTEXT://kafka-4.example:9092
log.dirs=/var/lib/kafka/data
```

Before the first start, every node's storage must be **formatted** with the same cluster id:
```bash
KAFKA_CLUSTER_ID=$(kafka-storage.sh random-uuid)
kafka-storage.sh format -t $KAFKA_CLUSTER_ID -c server.properties
```
This writes `meta.properties` (cluster id, node id). A node with a different cluster id refuses to join, which
protects you from connecting a broker to the wrong cluster.

**Static vs dynamic quorum:** with `controller.quorum.voters` the controller set is fixed in config. Since 3.9
(KIP-853), with `controller.quorum.bootstrap.servers` you can add or remove controllers at runtime
(`kafka-metadata-quorum.sh add-controller` / `remove-controller`).

---

## 5. The metadata log (`__cluster_metadata`)

A special internal topic with **one partition**, stored on the controllers (and copied by every broker). Each change
to the cluster is one **record**:

| Record | Meaning |
|---|---|
| `RegisterBrokerRecord` | broker 4 joined: its id, epoch, listeners, rack |
| `FenceBrokerRecord` / `UnfenceBrokerRecord` | broker 4 may not / may now lead partitions |
| `UnregisterBrokerRecord` | broker removed for good |
| `TopicRecord` | topic `orders` exists, topic id `9xj…` |
| `PartitionRecord` | `orders-0`: replicas [4,5,6], ISR [4,5,6], leader 4, leader epoch 0 |
| `PartitionChangeRecord` | `orders-0`: new leader 5, leader epoch 1, ISR [5,6] |
| `RemoveTopicRecord` | topic deleted |
| `ConfigRecord` | `orders` retention.ms = 86400000 |
| `ProducerIdsRecord` | next block of producer ids given out |
| `AccessControlEntryRecord` | an ACL |
| `FeatureLevelRecord` | `metadata.version` = 4.0-IV3, for example (which features the cluster may use) |

Example: the log after creating a topic and losing a broker:
```
offset  record
  101   RegisterBrokerRecord      broker 6, epoch 230, rack eu-1c
  102   UnfenceBrokerRecord       broker 6
  103   TopicRecord               orders  id=9xj…
  104   PartitionRecord           orders-0 replicas [4,5,6] isr [4,5,6] leader 4 epoch 0
  105   PartitionRecord           orders-1 replicas [5,6,4] isr [5,6,4] leader 5 epoch 0
  106   PartitionRecord           orders-2 replicas [6,4,5] isr [6,4,5] leader 6 epoch 0
  ...
  250   FenceBrokerRecord         broker 4            (missed heartbeats)
  251   PartitionChangeRecord     orders-0 leader 5 epoch 1 isr [5,6]
  252   PartitionChangeRecord     orders-1 isr [5,6]
  253   PartitionChangeRecord     orders-2 isr [6,5]
```

Every node that has read up to offset 253 has **exactly the same view** of the cluster. A broker's "metadata cache" is
the log applied up to its offset. If it has only read up to 250, it knows that it is behind and by how much.

---

## 6. Raft: how the controllers agree

Raft is a consensus algorithm: a way for a group of servers to agree on one ordered log, even when some of them fail.
KRaft's version (KIP-595) is shaped like Kafka replication: **followers pull** from the leader with `Fetch`.

### Terms
- **Epoch** (Raft calls it the "term"): a number that goes up with every election. At most one leader per epoch.
- **Voters**: the controllers. Each one is a **leader**, **follower** or **candidate**.
- **Observers**: brokers. They fetch the log but **don't vote** and never become leader.
- **High watermark**: the offset replicated to a **majority** of voters = committed. Only committed records are
  applied by anyone.

### Writing a record
```
1. active controller (leader, epoch 7) appends PartitionChangeRecord at offset 251
2. follower controllers 2 and 3 Fetch(offset 251) → copy it
3. majority (leader + 1 follower = 2 of 3) has it → high watermark = 252 → committed
4. the controller applies it; brokers fetch it and apply it too
```
Compare with partition replication (`partition.md`, section 13): there the HW waits for **all ISR** replicas, while
Raft waits for a **majority**. A majority means the quorum keeps working even if 1 of 3 controllers is slow, without any ISR.

### Electing a leader
- Followers expect to hear from the leader (fetch responses) regularly. If nothing comes for
  `controller.quorum.fetch.timeout.ms` (2 s), a follower becomes a **candidate**.
- The candidate increments the epoch, votes for itself, and sends **`Vote`** requests to the other voters.
- A voter grants its vote only if (a) it hasn't voted in this epoch yet **and** (b) the candidate's log is **at
  least as up to date** as its own (last epoch, then last offset). Rule (b) guarantees that the new leader has
  every committed record.
- **Majority of votes** → leader. It sends `BeginQuorumEpoch` to everyone. A random election timeout
  (`controller.quorum.election.timeout.ms`, 1 s, plus jitter) makes it unlikely that two candidates always tie.
- A leader that shuts down cleanly sends `EndQuorumEpoch`, so the next election starts immediately.

---

## 7. Leader election example

```
controllers 1, 2, 3. Leader = 2 in epoch 7. Logs: all end at offset 300 (epoch 7)

💥 controller 2 dies
t=0     1 and 3 stop getting fetch responses
t=2.0s  controller 3 times out first → candidate, epoch 8, votes for itself
        3 → Vote(epoch 8, last epoch 7, last offset 300) → controller 1
        1: haven't voted in epoch 8 ✔, 3's log is at least as long as mine ✔ → grant
t=2.0s  3 has 2 of 3 votes → LEADER of epoch 8 → BeginQuorumEpoch → 1
        brokers' fetches to 2 fail → they find the new leader → fetch from 3
t≈2.1s  3 is the active controller. It has all metadata in memory already: no loading
```

Nothing committed is lost: a committed record is on a majority, and any majority that elects the new leader
includes at least one node with that record. Rule (b) makes sure the winner has it.

When controller 2 comes back, it learns about epoch 8, becomes a follower, **truncates** anything it wrote but never
committed (like follower truncation with leader epochs), and catches up.

---

## 8. Brokers: registration, heartbeats, fencing

```
broker 4 starts
  → BrokerRegistration(node 4, listeners, rack, incarnation id) → active controller
  ← broker epoch 230 (a new number on every start = this run of the broker)
  → RegisterBrokerRecord in the log. The broker is FENCED (can't lead anything yet)
  → fetch __cluster_metadata until caught up with the log
  → BrokerHeartbeat(epoch 230, metadata offset 253, "I'm caught up")
  ← "unfenced" → UnfenceBrokerRecord → it can now lead partitions
  → BrokerHeartbeat every broker.heartbeat.interval.ms (2 s)
```

| State | Meaning |
|---|---|
| **fenced** | registered but not allowed to lead partitions or be in the ISR: just started, behind on metadata, or missed heartbeats |
| **unfenced** | normal operation |
| **controlled shutdown** | asked to stop: the controller moves its leaderships away first, then fences it |

If the controller gets no heartbeat for `broker.session.timeout.ms` (9 s), it **fences** the broker and
moves leadership of its partitions to other ISR members. This replaces ZooKeeper's ephemeral `/brokers/ids/4` node and
session expiry.

The **broker epoch** is fencing for brokers: if an old run of broker 4 (frozen by a GC pause) wakes up, its requests carry
the old epoch 230, while the new run has 231, so they are rejected.

---

## 9. What the active controller does

All of these are "write records to the metadata log":

- **Broker membership**: register, fence/unfence, controlled shutdown
- **Topic management**: CreateTopics, DeleteTopics, CreatePartitions (forwarded by brokers)
- **Replica placement**: choose replicas for new partitions, spreading leaders and racks
- **Leader election**: when a leader is fenced, pick a new leader from the ISR, leader epoch + 1
- **ISR changes**: a partition leader sends `AlterPartition` ("remove 6 from the ISR of orders-0") and the controller
  checks it and writes a `PartitionChangeRecord`
- **Preferred leader election**: every `leader.imbalance.check.interval.seconds` (300) it moves leaders back to the
  preferred replica
- **Reassignments**: `kafka-reassign-partitions.sh`
- **Configs, ACLs, quotas, feature versions, producer id blocks**

The controller never moves data. It only **decides** and records the decision. Brokers act on it: create logs, become
leader or follower, start fetching.

---

## 10. Example: a broker dies

```
cluster: brokers 4, 5, 6. orders-0: replicas [4,5,6], leader 4, ISR [4,5,6], epoch 0

💥 broker 4 crashes
t=0      heartbeats from 4 stop
t=9s     controller: session timeout → FenceBrokerRecord(4)
         for every partition 4 led: new leader from the ISR, removed from the ISR:
           PartitionChangeRecord orders-0: leader 5, epoch 1, ISR [5,6]
t=9s+    brokers 5, 6 fetch these records:
           5: "I'm leader of orders-0 epoch 1" → accept produce requests
           6: "the leader is 5 now" → start fetching orders-0 from 5
         producers get NOT_LEADER_OR_FOLLOWER from … (4 is dead: connection errors)
         → refresh metadata → send to 5
t=later  4 restarts → registers with a NEW broker epoch → fenced → catches up on metadata
         → unfenced → truncates its logs if needed → fetches as a follower → rejoins the ISR
         → preferred leader election moves orders-0 back to 4
```

A **clean** shutdown is much smoother: the broker asks for controlled shutdown, the controller moves its leaders away
**before** it stops, and clients just follow.

---

## 11. Example: a topic is created

```
admin → CreateTopics(orders, 3 partitions, RF 3) → broker 5 (any broker)
broker 5 → forwards → active controller 3
controller 3:
  validate: name ok? exists? enough unfenced brokers for RF 3?
  place replicas (rack-aware): orders-0 [4,5,6], orders-1 [5,6,4], orders-2 [6,4,5]
  write TopicRecord + 3 × PartitionRecord → wait until committed (majority)
  → response to broker 5 → response to admin
brokers 4, 5, 6 fetch the records:
  "I'm a replica of orders-0" → create /var/lib/kafka/data/orders-0/ → leader or follower
```

---

## 12. Snapshots

The metadata log would grow forever, and a new broker would have to replay millions of records. So controllers and
brokers regularly write a **snapshot**: the full state at one offset (file `<offset>-<epoch>.checkpoint`).
After that, the log before that offset can be deleted.

- A new or far-behind broker gets the latest snapshot (`FetchSnapshot`), then fetches the log after it.
- A new snapshot is taken after `metadata.log.max.record.bytes.between.snapshots` (20 MB) of new records,
  or after `metadata.log.max.snapshot.interval.ms` (1 h).

This is the same idea as compaction in `partition.md`: keep the current state, drop the history.

---

## 13. Files on disk

```
/var/lib/kafka/meta/                                 (on a controller; brokers have the same directory in their log.dirs)
├── meta.properties                                  cluster.id, node.id, version
└── __cluster_metadata-0/
    ├── 00000000000000000000.log                     metadata records (normal segments)
    ├── 00000000000000000000.index
    ├── 00000000000000000000.timeindex
    ├── 00000000000001023456-0000000008.checkpoint   snapshot at offset 1023456, epoch 8
    ├── leader-epoch-checkpoint
    ├── partition.metadata
    └── quorum-state                                 current epoch, leader, who I voted for
```

`quorum-state` is how a controller remembers its vote across a restart. Without it, it could vote twice in the same
epoch, and two leaders could be elected.

---

## 14. Failure cases

| What fails | Effect |
|---|---|
| 1 of 3 controllers | nothing visible. If it was the leader, there's a new one in ~2 s |
| 2 of 3 controllers | **no majority**: no metadata changes. No new topics, and **no leader elections**. Existing leaders keep serving produce/fetch, but if a broker dies too, its partitions stay offline |
| a broker | fenced after 9 s, leaders moved (section 10) |
| network split: controller 1 alone, 2 + 3 together | 2 + 3 have a majority and elect a leader. Controller 1 can't commit anything, so its old leadership is useless (fenced by the higher epoch) |

---

## 15. ZooKeeper mode vs KRaft

| | ZooKeeper mode (≤ 3.x) | KRaft |
|---|---|---|
| Metadata store | ZooKeeper znodes | `__cluster_metadata` log |
| Controller | 1 broker, elected via ephemeral `/controller` | quorum of 3/5 controllers, Raft leader |
| Controller fencing | `/controller_epoch` | Raft epoch |
| Broker liveness | ephemeral `/brokers/ids/N` + ZooKeeper session (18 s) | `BrokerHeartbeat` + `broker.session.timeout.ms` (9 s) |
| Brokers learn changes | controller pushes `UpdateMetadata`, `LeaderAndIsr` | brokers fetch the log |
| Failover | new controller reads everything from ZooKeeper | already in memory |
| ISR change | leader writes ZooKeeper (`zkVersion` check) | leader sends `AlterPartition` to the controller |
| Scale | ~200k partitions | millions |
| Config | `zookeeper.connect` | `process.roles`, `controller.quorum.*` |

**Migration:** Kafka 3.4–3.9 can migrate a running ZooKeeper cluster to KRaft (a "dual-write" phase writes to both).
Kafka 4.0 can't run with ZooKeeper, so a ZooKeeper cluster must be migrated on 3.x **before** upgrading to 4.0.

---

## 16. simple-kafka vs KRaft

simple-kafka works like **ZooKeeper-mode Kafka**, simplified:

| | simple-kafka | KRaft |
|---|---|---|
| Who stores metadata | ZooKeeper (`/brokers`, `/topics`, `/controller`) | controller quorum, metadata log |
| Controller election | ephemeral `/controller` node, first wins | Raft vote, majority wins |
| Controller fencing | none (zombie controller possible) | epoch |
| Brokers learn about topics | `TOPIC_NOTIFICATION` + read ZooKeeper | read the log |
| Broker liveness | ZooKeeper session (30 s) | heartbeats (9 s) |
| Leader epoch | none | in every `PartitionChangeRecord` |

---

## 17. Try it

```bash
# who is the leader, epoch, high watermark, lag of each controller / observer
kafka-metadata-quorum.sh --bootstrap-server <broker>:9092 describe --status
kafka-metadata-quorum.sh --bootstrap-server <broker>:9092 describe --replication

# browse the metadata like a file system (offline, on a node's disk)
kafka-metadata-shell.sh --directory /var/lib/kafka/meta/__cluster_metadata-0
  >> ls /brokers
  >> cat /topics/orders/0/data

# decode the metadata log records
kafka-dump-log.sh --cluster-metadata-decoder --files /var/lib/kafka/meta/__cluster_metadata-0/00000000000000000000.log
```

To check whether your work cluster is KRaft: look for `process.roles` in the broker config (KRaft) or
`zookeeper.connect` (ZooKeeper mode).

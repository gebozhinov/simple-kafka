# ZooKeeper

General concepts. Method explanations of `ZookeeperClient` live in `stage-3-zookeeper.md`.
How Kafka works **today** without ZooKeeper: `kraft.md`.
Quiz: `quiz-2-cluster-and-zookeeper.md`, `quiz-4-cluster-internals.md`

**Why learn ZooKeeper if Kafka 4.0 removed it?** Because the problems it solved haven't gone away: membership, failure
detection, electing exactly one controller, fencing old leaders, keeping one ordered source of truth. KRaft solves the
same problems in the same way, just inside Kafka. Many clusters at work still run ZooKeeper mode on Kafka 3.x.

Contents:
1. What ZooKeeper is · 2. The data model · 3. Node types · 4. Sessions · 5. Watches ·
6. Guarantees · 7. The ensemble: leader, followers, observers · 8. Writes and reads (ZAB) ·
9. Configuration and files · 10. How real Kafka used ZooKeeper · 11. The ZooKeeper-mode controller ·
12. How simple-kafka uses it · 13. What the article says vs reality · 14. Why Kafka moved on · 15. Try it

---

## 1. What is ZooKeeper?

**ZooKeeper is a small, very reliable database that several programs share in order to coordinate.**
It's a separate server (default port **2181**), not part of Kafka. Kafka brokers used it to agree on things
that every broker must see the same way:

- which brokers are alive right now
- which broker is the controller
- which topics exist, and the leader, ISR and replicas of each partition
- configs, ACLs, quotas

Think of it as a **shared notice board** that all brokers can read and write, and that tells them when
something on it changes. It's used by many other systems too (HBase, Solr, older Hadoop).

**Clients (producers, consumers) never talk to ZooKeeper.** It's the brokers' private coordination system.
Clients only learn this information indirectly, through Metadata requests to brokers. (Very old consumers,
before Kafka 0.9, did talk to ZooKeeper; see section 13.)

---

## 2. The data model: a tree of znodes

Data is stored like a file system. Each node in the tree is a **znode**. A znode has:
- a **path**, like `/brokers/1`
- optional **data**, a small `byte[]` (meant for small values, max ~1 MB, `jute.maxbuffer`)
- optional **children**
- a **stat**: versions and ids that ZooKeeper maintains

Unlike a file system, a znode can have **both** data and children at the same time.

Example: simple-kafka with 3 brokers and one topic:
```
/
├── brokers
│   ├── 1             "localhost:9091"     (ephemeral)
│   ├── 2             "localhost:9092"     (ephemeral)
│   └── 3             "localhost:9093"     (ephemeral)
├── controller        "1"                  (ephemeral)
└── topics
    └── orders
        └── partitions
            ├── 0     "1;2,"               leader 1; followers 2
            ├── 1     "2;3,"               leader 2; followers 3
            └── 2     "3;1,"               leader 3; followers 1
```

### The stat of a znode
```
stat /controller
cZxid = 0x200000014        zxid of the transaction that created it
mZxid = 0x200000014        zxid of the last change
ctime / mtime              when created / last modified
dataVersion = 0            +1 on every setData   ← used by setData(path, data, version)
cversion = 0               +1 when children change
ephemeralOwner = 0x10002a5c3b40001   session that owns it (0 = persistent)
dataLength = 1
numChildren = 0
```
`ephemeralOwner` tells you **which session** created an ephemeral node. That's how you find out which broker connection
holds `/controller`.

---

## 3. Node types

| Type | Lives until | Used for |
|------|-------------|----------|
| **Persistent** | someone deletes it | topics, partitions: data that must survive restarts |
| **Ephemeral** | the session that created it ends | "I'm alive" markers: `/brokers/1`, `/controller` |
| **Sequential** (+ persistent or ephemeral) | as above; ZooKeeper appends a counter: `lock-0000000007` | queues, fair locks (Kafka used it for `/isr_change_notification`) |
| **Container** (3.5+) | deleted automatically when its last child is gone | parent folders for locks |
| **TTL** (3.5+) | deleted if unchanged for a time and has no children | temporary data |

**Ephemeral nodes are the key idea.** A broker doesn't have to say "I died", which it can't do if it crashed.
ZooKeeper notices that the broker's session is gone and removes the node by itself.
Ephemeral nodes **can't have children**.

---

## 4. Sessions: how ZooKeeper knows a client is alive

When a client connects, it gets a **session** (a 64-bit session id + password) with a timeout. The client library sends
heartbeats (pings) in the background, every ~1/3 of the timeout.

The timeout is **negotiated**: the server limits it to between `2 × tickTime` and `20 × tickTime` (with the
default `tickTime=2000` ms: 4 s to 40 s). simple-kafka asks for 30 s. Kafka brokers in ZooKeeper mode used
`zookeeper.session.timeout.ms=18000`.

| State | What it means | Ephemeral nodes |
|-------|---------------|-----------------|
| **Disconnected** | connection lost, but the session may still be alive on the server. The library reconnects automatically (possibly to another ZooKeeper server) with the same session id | still there |
| **Expired** | no heartbeat for longer than the timeout; the server ended the session | **deleted** |

Example timeline, broker 1's network drops:
```
10:00:00  connection lost                  → Disconnected. /brokers/1 still exists
10:00:05  network back, reconnects         → SyncConnected, same session, nothing lost
---------
10:00:00  connection lost                  → Disconnected
10:00:30  no heartbeat for 30 s            → server expires the session, deletes /brokers/1
          other brokers' watches fire      → "broker 1 is gone"
10:00:40  network back                     → Expired. The client must open a NEW session
                                             and re-create its ephemeral nodes itself
```

**Important:** a client finds out about expiry only **after it reconnects**. During the gap, the client can't tell
whether its session is still alive, so it might still think it's the controller (the zombie controller, Q39).
That's why fencing with epochs is needed.

Session expiry is decided by the **ZooKeeper leader**, so all servers agree on it.

---

## 5. Watches: "tell me when this changes"

A client can ask to be notified when a znode changes, by passing a watcher to a read:

| Read | Watch fires on |
|---|---|
| `exists(path, watch)` | `NodeCreated`, `NodeDeleted`, `NodeDataChanged` |
| `getData(path, watch)` | `NodeDeleted`, `NodeDataChanged` |
| `getChildren(path, watch)` | `NodeChildrenChanged` (child added/removed), `NodeDeleted` |

**Watches are one-time.** After a watch fires once, it's gone. To keep watching, you must register it again,
usually by reading the node again with a new watch. Changes that happen between the event and the
re-registration are not lost, because you re-read the current state, but you get one event for them, not one per change.

**The event carries no data.** It only says "`/brokers` children changed". You must read again to see what changed.

ZooKeeper guarantees a client sees the watch event **before** it sees the new data, so it never reads a change and
misses its event.

Watches belong to a session: if the session expires, all its watches are gone too.
ZooKeeper 3.6+ also has **persistent watches** (`addWatch`, they don't need re-registering), but Kafka didn't use them.

---

## 6. Guarantees that make coordination possible

- **Only one wins:** if two clients create the same path at the same moment, exactly one succeeds; the other
  gets `NodeExistsException`. This is how leader/controller election works.
- **Same order for everyone (linearizable writes):** all writes go through one leader and get a global order, the zxid.
  All clients see changes in that order.
- **FIFO per client:** one client's requests are applied in the order it sent them.
- **Versions:** every znode has a version number that increases on every change. A write can say
  "only if the version is still 5" (optimistic locking), so two clients can't silently overwrite each other.
- **Atomic multi:** `multi()` applies several operations all or nothing (Kafka used it for controller writes, see 11).
- **Reads may be stale.** A read is served by the server you're connected to, which may be slightly behind
  the leader. Call `sync(path)` before the read if you need the latest value.

---

## 7. The ensemble: leader, followers, observers

In production ZooKeeper runs as an **ensemble** of 3 or 5 servers, each with a full copy of the tree in memory.

| Role | Votes | Does |
|---|---|---|
| **Leader** | ✔ | orders all writes, decides session expiry |
| **Follower** | ✔ | serves reads, forwards writes to the leader, votes |
| **Observer** | ✗ | serves reads, forwards writes, **doesn't vote**: adds read capacity (e.g. in another data centre) without slowing writes |

A write is accepted when a **majority** (quorum) of voters has it:

| Servers | Majority | Can lose |
|---------|----------|----------|
| 1 | 1 | 0 |
| 3 | 2 | 1 |
| 5 | 3 | 2 |

That's why it's always an odd number: 4 servers need 3 for a majority and can still lose only 1, same as 3 servers.
If the majority is lost, ZooKeeper stops accepting writes rather than letting two halves disagree
(same "consistency over availability" choice as Kafka's unclean leader election default).

---

## 8. Writes and reads (ZAB)

ZooKeeper's consensus protocol is **ZAB** (ZooKeeper Atomic Broadcast). It's a different algorithm from Raft, with the same goal.

### The zxid
Every write gets a 64-bit **zxid** = `epoch (high 32 bits) | counter (low 32 bits)`:
```
0x0000000200000014   →  epoch 2, write number 0x14 (20) in that epoch
```
The **epoch** goes up with every new ZooKeeper leader, just like Kafka's controller epoch and leader epoch. The
same fencing idea appears again.

### A write
```
client → follower B: create /brokers/2
B → leader A: forward
A: assign zxid 0x200000015 → PROPOSAL to all followers
followers: write it to their transaction log on disk (fsync!) → ACK
A: majority ACKed → COMMIT → all servers apply it to their in-memory tree
B → client: OK
```
Every write is **fsynced to disk on a majority** before it's committed. That's why ZooKeeper needs a fast disk and
handles only thousands of writes per second, which is fine for metadata but wrong for messages.

### Leader election in ZooKeeper
When the leader dies, servers vote for the server with the **highest zxid** (the most complete history), with the server id
as the tie-breaker. The winner starts a new epoch and syncs the followers to its history.
This is the same rule as Raft's "the candidate's log must be at least as up to date".

---

## 9. Configuration and files

```properties
# zoo.cfg
tickTime=2000            # base time unit (ms)
initLimit=10             # ticks a follower may take to connect + sync with the leader
syncLimit=5              # ticks a follower may lag before it's dropped
dataDir=/var/lib/zookeeper
clientPort=2181
server.1=zk1:2888:3888   # 2888 = follower ↔ leader, 3888 = leader election
server.2=zk2:2888:3888
server.3=zk3:2888:3888
```
`dataDir/myid` contains this server's number (1, 2, 3).

On disk:
- **transaction log** (`version-2/log.*`): every write, appended and fsynced
- **snapshots** (`version-2/snapshot.*`): the whole tree at some zxid. At startup: load the latest snapshot, replay the
  log after it. (Same idea as KRaft snapshots and Kafka segments.)

Monitoring: `echo ruok | nc zk1 2181` → `imok`, `echo mntr | nc zk1 2181` (needs `4lw.commands.whitelist`), or the
AdminServer on port 8080.

---

## 10. How real Kafka used ZooKeeper

The real znode tree of a Kafka cluster in ZooKeeper mode:

```
/
├── cluster/id                                    {"version":"1","id":"Xk3...Q"}
├── controller                                    {"version":2,"brokerid":1,"timestamp":"..."}   ephemeral
├── controller_epoch                              "23"
├── brokers
│   ├── ids
│   │   ├── 1                                     ephemeral, JSON: endpoints, rack, jmx_port
│   │   │   {"listener_security_protocol_map":{"PLAINTEXT":"PLAINTEXT"},
│   │   │    "endpoints":["PLAINTEXT://kafka-1:9092"],"rack":"eu-1a","host":"kafka-1","port":9092,"version":5}
│   │   ├── 2
│   │   └── 3
│   ├── topics
│   │   └── orders                                {"version":3,"topic_id":"9xj...","partitions":{"0":[1,2,3],"1":[2,3,1],"2":[3,1,2]}}
│   │       └── partitions
│   │           └── 0
│   │               └── state                     {"controller_epoch":23,"leader":1,"leader_epoch":5,"isr":[1,2,3],"version":1}
│   └── seqid
├── config
│   ├── topics/orders                             {"version":1,"config":{"retention.ms":"86400000"}}
│   ├── brokers/<id>  clients/<id>  users/<name>  (dynamic configs, quotas, SCRAM credentials)
│   └── changes/config_change_0000000042          sequential: "something changed, re-read"
├── admin
│   ├── delete_topics/orders                      pending deletion
│   ├── reassign_partitions                       running reassignment
│   └── preferred_replica_election
├── isr_change_notification/isr_change_0000000007 sequential: "ISR of these partitions changed"
├── log_dir_event_notification                    "a disk failed on broker 2"
├── kafka-acl/Topic/orders                        ACLs
└── consumers/                                    only very old consumers (< 0.9)
```

Compared with simple-kafka:
- **Assignment** (`/brokers/topics/orders`) and **state** (`.../partitions/0/state`) are separate. The assignment says which
  replicas exist and rarely changes. The state (leader, epoch, ISR) changes on every failure.
- The state includes **`leader_epoch`** and **`controller_epoch`**: fencing is built into the data.
- Broker data is **JSON with a version field**, so the format can evolve. Our `"1;2,"` can't.

---

## 11. The ZooKeeper-mode controller

**Election:** exactly as in simple-kafka. Every broker tries to create the ephemeral `/controller`, and the winner
**increments `/controller_epoch`**.

**At startup (failover)** the new controller **reads everything** from ZooKeeper: all brokers, all topics, all partition
states. With 200k partitions that took **minutes**, and during that time no leader elections could happen. This was the main
reason for KRaft.

**Then it watches** `/brokers/ids` (brokers come and go), `/brokers/topics` (new topics), `/admin/*`
(delete, reassign), `/isr_change_notification`.

**How it told brokers what to do: RPCs, pushed from the controller:**
| Request | Meaning |
|---|---|
| `LeaderAndIsr` | "for orders-0 you are now leader (epoch 6) / follower of broker 2" |
| `UpdateMetadata` | "here's the new metadata, update your cache" (so brokers can answer clients) |
| `StopReplica` | "stop and optionally delete your replica of orders-0" |

Each request carried the **controller epoch**. Brokers rejected requests with an older epoch (`STALE_CONTROLLER_EPOCH`).

**How ISR changes worked:** the partition **leader** (not the controller) updated `.../partitions/0/state` in ZooKeeper with
a conditional write on the znode's version (`zkVersion`). If the version had changed (the controller had just changed
the leader), the write failed and the leader knew it was out of date. Then it created an `isr_change_notification`
so the controller would send `UpdateMetadata` to everyone. Later versions (2.7+) replaced this with an
`AlterIsr` request to the controller, which is today's `AlterPartition`.

**Fencing ZooKeeper writes:** an old controller could still write to ZooKeeper. From Kafka 2.x, controller writes were
`multi()` operations that **also checked the version of `/controller_epoch`**. If another controller had been elected, the
version had changed and the whole write failed.

---

## 12. How simple-kafka uses it, step by step

### 1. A broker starts and registers
```
broker 2 starts
  → connect()                              opens a session
  → createEphemeralNode("/brokers/2", "localhost:9092")
  → watchChildren("/brokers", ...)         "tell me when brokers come and go"
```

### 2. Controller election
All brokers try to create the same ephemeral node. ZooKeeper lets only one succeed:
```
broker 1: createEphemeralNode("/controller", "1")  → success → I'm the controller
broker 2: createEphemeralNode("/controller", "2")  → already exists → controller is 1
          watchNode("/controller", ...)             → tell me if it changes
broker 3: same as broker 2
```

### 3. A broker dies
```
broker 3 crashes
  → 30 s later its session expires → ZooKeeper deletes /brokers/3
  → watches on /brokers fire on brokers 1 and 2: children are now [1, 2]
  → the controller (broker 1) moves leadership of broker 3's partitions to other brokers
    and updates /topics/.../partitions/... in ZooKeeper
```

### 4. The controller dies
```
broker 1 crashes
  → its session expires → /controller is deleted (and /brokers/1)
  → watches on /controller fire on brokers 2 and 3
  → both try createEphemeralNode("/controller", ...) → one wins, e.g. broker 2
  → the loser watches /controller again
```

### 5. A topic is created
```
client → create_topic("orders", 3, 2) → controller
controller → createPersistentNode("/topics/orders/partitions/0", "1;2,") ... for each partition
controller → TOPIC_NOTIFICATION("orders") → other brokers
other brokers → getChildren / getData under /topics/orders → create the partitions on disk
```

### What simple-kafka is missing compared to real ZooKeeper-mode Kafka
| Missing | Consequence |
|---|---|
| `/controller_epoch` | zombie controller can overwrite decisions (Q39) |
| `leader_epoch` in partition state | an old leader can keep accepting writes |
| conditional writes (`-1` everywhere) | lost updates when two brokers write the same node (Q37) |
| separate assignment and state | — |
| JSON with versions | the data format can't change |

---

## 13. What the article says vs reality

| The article says | Reality |
|---|---|
| ZooKeeper stores **consumer group offsets** in persistent nodes | only consumers **before Kafka 0.9** (2015). Since then offsets are in the `__consumer_offsets` topic, written through the group coordinator |
| child watches track **consumer group membership** | same: old consumers only. Today membership is handled by the group coordinator broker (`consumers.md`) |
| brokers register under `/brokers/ids/` | ✔ in real Kafka. simple-kafka uses `/brokers/` directly |
| per-partition state nodes hold leader, ISR and replicas | ✔ `/brokers/topics/<t>/partitions/<p>/state` (+ leader epoch, controller epoch) |
| brokers watch partition state nodes to learn if they're leader | ✗ in real Kafka the **controller pushed** `LeaderAndIsr` requests. Brokers didn't watch every partition |
| Kafka uses ZooKeeper for discovery, config and leader election | ✔ until 3.x; since 4.0 it's all KRaft (`kraft.md`) |

---

## 14. Why Kafka moved on

Short version (details in `kraft.md`, section 2):
- a second distributed system to install, monitor, secure and upgrade
- slow controller failover: the new controller had to reload everything from ZooKeeper, which limited clusters to ~200k partitions
- pushed metadata RPCs that a broker could miss, and two sources of truth (ZooKeeper + controller memory)

KRaft (production-ready in Kafka 3.3, ZooKeeper removed in **4.0**) keeps the **ideas** from this file (ephemeral
membership → heartbeats, `/controller` election → Raft vote, `/controller_epoch` → Raft epoch, znodes → metadata
records, watches → fetching the log) and builds them into Kafka.

---

## 15. Try it yourself

```bash
docker run -d --rm --name zk -p 2181:2181 zookeeper:3.8
docker exec -it zk zkCli.sh            # ZooKeeper's command-line client

ls /                                   # children of the root
create /demo "hello"                   # persistent node
get /demo                              # → hello
set /demo "world"
stat /demo                             # version, zxids, owner session, ...
set -v 0 /demo "again"                 # conditional write: fails, the version is now 1
create -e /alive "me"                  # ephemeral: disappears when you quit zkCli
create -s /queue/item- "x"             # sequential: /queue/item-0000000000 (create /queue first)
ls -w /                                # list + watch: next child change prints an event
delete /demo
quit

docker exec zk zkServer.sh status      # mode: standalone / leader / follower
docker rm -f zk
```

On a ZooKeeper-mode Kafka cluster (3.x): `zookeeper-shell.sh zk1:2181 get /controller` or
`ls /brokers/ids` show you the real tree from section 10.

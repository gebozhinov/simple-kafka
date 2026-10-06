# ZooKeeper

General concepts. Method explanations of `ZookeeperClient` live in `stage-3-zookeeper.md`.

---

## What is ZooKeeper?

**ZooKeeper is a small, very reliable database that several programs share in order to coordinate.**
It's a separate server (default port **2181**), not part of Kafka. Kafka brokers used it to agree on things
that every broker must see the same way:

- which brokers are alive right now
- which broker is the controller
- which topics exist, and which broker leads each partition

Think of it as a **shared notice board** that all brokers can read and write, and that tells them when
something on it changes.

**Clients (producers, consumers) never talk to ZooKeeper.** It's the brokers' private coordination system.
Clients only learn this information indirectly, through metadata requests to brokers.

---

## The data model: a tree of znodes

Data is stored like a file system. Each node in the tree is a **znode**. A znode has:
- a **path**, like `/brokers/1`
- optional **data**, a small `byte[]` (meant for small values, max ~1 MB)
- optional **children**

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

---

## Three node types

| Type | Lives until | Used for |
|------|-------------|----------|
| **Persistent** | someone deletes it | topics, partitions: data that must survive restarts |
| **Ephemeral** | the session that created it ends | "I'm alive" markers: `/brokers/1`, `/controller` |
| **Sequential** (+ persistent or ephemeral) | as above; ZooKeeper appends a counter: `lock-0000000007` | queues, fair locks (simple-kafka doesn't use it) |

**Ephemeral nodes are the key idea.** A broker doesn't have to say "I died", which it can't do if it crashed.
ZooKeeper notices that the broker's session is gone and removes the node by itself.

---

## Sessions: how ZooKeeper knows a client is alive

When a client connects, it gets a **session** with a timeout (simple-kafka: 30 s). The client library sends
heartbeats in the background. Two different situations:

| State | What it means | Ephemeral nodes |
|-------|---------------|-----------------|
| **Disconnected** | connection lost, but the session may still be alive on the server. The library reconnects automatically (possibly to another ZooKeeper server) | still there |
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

---

## Watches: "tell me when this changes"

A client can ask to be notified when a znode changes (data changed, created, deleted) or when its children
change (child added/removed). ZooKeeper calls the client back with an event.

**Watches are one-time.** After a watch fires once, it's gone. To keep watching, you must register it again,
usually by reading the node again with a new watch. Changes that happen between the event and the
re-registration are not lost, because you re-read the current state, but you get one event for them, not one per change.

Watches belong to a session: if the session expires, all its watches are gone too.

---

## Guarantees that make coordination possible

- **Only one wins:** if two clients create the same path at the same moment, exactly one succeeds; the other
  gets `NodeExistsException`. This is how leader/controller election works.
- **Same order for everyone:** all clients see changes in the same order.
- **Versions:** every znode has a version number that increases on every change. A write can say
  "only if the version is still 5" (optimistic locking), so two clients can't silently overwrite each other.

---

## Ensemble and quorum

In production ZooKeeper runs as an **ensemble** of 3 or 5 servers, each with a full copy. A write is accepted
when a **majority** (quorum) has it:

| Servers | Majority | Can lose |
|---------|----------|----------|
| 1 | 1 | 0 |
| 3 | 2 | 1 |
| 5 | 3 | 2 |

That's why it's always an odd number: 4 servers need 3 for a majority and can still lose only 1, same as 3 servers.
If the majority is lost, ZooKeeper stops accepting writes rather than letting two halves disagree
(same "consistency over availability" choice as Kafka's unclean leader election default).

---

## How simple-kafka uses it, step by step

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

---

## Real Kafka with ZooKeeper (before KRaft)

Same ideas, different paths:
```
/brokers/ids/1                  ephemeral, broker host/port as JSON
/brokers/topics/orders          persistent, partition → replicas assignment
/controller                     ephemeral, {"brokerid":1,...}
/controller_epoch               counter, increases with every new controller
/admin/delete_topics            pending topic deletions
/config/topics/orders           per-topic config overrides
```
Broker config: `zookeeper.connect=zk1:2181,zk2:2181,zk3:2181`, `zookeeper.session.timeout.ms` (default 18 s).

`/controller_epoch` solves a problem simple-kafka has: an old controller that was frozen (long GC pause) and
comes back may still think it's the controller. Every controller decision carries the epoch number, and brokers
ignore decisions with an older epoch. This is called **fencing**.

### Why Kafka replaced it (KRaft)
- a second distributed system to install, monitor, secure and upgrade
- metadata changes went through ZooKeeper, so a controller failover in a big cluster had to reload everything:
  slow, and it limited clusters to roughly 200k partitions
- two sources of truth (ZooKeeper and the controller's memory) that could drift apart

KRaft (production-ready in Kafka 3.3, ZooKeeper removed in **4.0**): a few Kafka nodes act as controllers and
keep the metadata in an internal Kafka log, `__cluster_metadata`, replicated with the Raft algorithm.
Brokers fetch that log like followers fetch a partition. Same concepts: membership, controller election,
one source of truth. They're built into Kafka now.

---

## Try it yourself

```bash
docker run -d --rm --name zk -p 2181:2181 zookeeper:3.8
docker exec -it zk zkCli.sh            # ZooKeeper's command-line client

ls /                                   # children of the root
create /demo "hello"                   # persistent node
get /demo                              # → hello
set /demo "world"
stat /demo                             # version, creation time, owner session, ...
create -e /alive "me"                  # ephemeral: disappears when you quit zkCli
ls -w /                                # list + watch: next child change prints an event
delete /demo
quit

docker rm -f zk
```

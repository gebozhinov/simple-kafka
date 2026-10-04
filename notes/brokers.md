# Brokers, Leaders and Replicas

General concepts about brokers. Method explanations live in the `stage-N-*.md` files.

---

## What is a broker?

A **broker is one Kafka server**: a running program (a JVM process) on a machine, listening on a
TCP port (usually 9092). It:
1. **receives** messages from producers (Produce requests) and **writes them to disk**
2. **sends** stored messages to consumers (Fetch requests)
3. **answers questions** about the cluster (Metadata requests)

Producers and consumers never talk to each other. They only talk to brokers.
The broker is the "post office" in the middle.

```
Producer ──Produce──▶  BROKER  ──Fetch──▶ Consumer
                      (stores messages on disk)
```

The producer and consumer don't need to run at the same time: the broker keeps the message
until the consumer asks for it.

Several brokers working together = a **Kafka cluster**. Partitions are spread across them, so
the load is shared and a copy survives if one broker dies.

On disk each partition is a folder of log files, e.g. `/var/lib/kafka/data/orders-0/00000000000000000000.log`
(config `log.dirs`).

In simple-kafka the broker is `SimpleKafkaBroker.java`: it opens a socket, reads the bytes produced by
our `encode...Request` methods, decodes them, and writes/reads messages in `Partition.java`.
`BrokerInfo` (id, host, port) is how one broker is described to clients.

---

## Partition leaders and replicas

### Replicas
Each partition is stored on **several brokers** so the data survives if one dies.
How many copies = **replication factor** (set when the topic is created).

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

(The one cluster-wide special role is the **controller**: the broker that decides who leads what.
If it dies, another broker takes over that job. It's a different thing from partition leadership.)

### Leader vs follower
- **Leader**: the only copy that producers write to and (normally) consumers read from.
  It decides the order of messages and gives them their offsets.
- **Follower**: just keeps a copy. It **fetches from the leader**, exactly like a consumer does
  (it sends Fetch requests), and appends what it gets to its own log.

Why only one leader? If two brokers accepted writes for the same partition, they could give
offset 5 to two different messages. One leader = one order, one truth.

Leaders are **spread across brokers** (see the table) so every broker does some of the work.

### Example: a message is written
`producer.send("orders", partition 1, "hi")`:
1. metadata says leader of orders-1 = broker 2 → producer sends to broker 2
2. broker 2 appends "hi" at offset 7
3. broker 3 (follower) fetches from broker 2: "anything after offset 6?" → gets "hi" at offset 7
4. now both copies have it

### In-sync replicas (ISR)
Followers that are caught up with the leader are **in sync**. The ISR list = leader + caught-up followers.
A follower that falls behind for longer than `replica.lag.time.max.ms` (default 30 s) is removed from the ISR.

### Example: a broker dies (failover)
```
💥 broker 2 dies
  orders-0 → leader still broker 1 (only lost a follower, nothing changes for clients)
  orders-1 → leader moves to broker 3, ISR [2, 3] → [3]
  orders-2 → leader still broker 3
```
1. the controller notices broker 2 is gone
2. for each partition broker 2 led, it picks a new leader **from the ISR** (it has all the data)
3. clients sending to broker 2 get errors → refresh metadata → learn the new leader → continue
4. when broker 2 comes back, it becomes a **follower**, catches up, rejoins the ISR, and later gets
   its leadership back (`auto.leader.rebalance.enable`)

Partitions led by other brokers keep working without a pause. Clients of `orders-1` see a short
pause and some retries.

### Why `acks` matters here (producer config)
- `acks=0`: don't wait at all. Fastest, can lose messages.
- `acks=1`: leader wrote it. If the leader dies before followers copied it → **message lost**.
- `acks=all`: all ISR replicas have it. Survives a leader crash. (Default since Kafka 3.0.)

`acks=all` + topic `min.insync.replicas=2` = "only accept a write if at least 2 copies have it".
If the ISR shrinks below 2, producers get `NotEnoughReplicasException` instead of silently
writing to just one copy. Typical safe setup: replication factor 3, `min.insync.replicas=2`, `acks=all`.

### Gotcha: unclean leader election
If **all** ISR replicas are dead, Kafka can either wait for one to come back (default,
`unclean.leader.election.enable=false`), or pick an out-of-sync follower as leader → messages it
didn't have yet are **lost**. Default chooses consistency over availability.

See it on a real cluster: `kafka-topics.sh --describe --topic <name>` shows the leader, replicas and ISR
of every partition.

---

## Which broker does a client talk to?

Produce and fetch go to the **leader of that partition**. A consumer reading 3 partitions led by
3 different brokers has connections to **all 3 brokers**.

How the client finds the leader:
1. `bootstrap.servers` gives it a few brokers to start with (any of them, just to get in)
2. it sends a **Metadata request** → learns all brokers (id, host, port) and the leader of each partition
3. it connects directly to the right leader for each partition
4. if the leader dies → error `NOT_LEADER_OR_FOLLOWER` → refresh metadata → go to the new leader

Different brokers can have different jobs for the same consumer:
- **partition leader** → where it fetches messages
- **group coordinator** → where it commits offsets (`__consumer_offsets`)

(Since Kafka 2.4, with `client.rack` set, a consumer can fetch from a follower in its own zone
to save cross-zone traffic.)

---

## What can cause a broker to "die"?

"Dead" for the cluster means **the controller stopped hearing from the broker** (missed heartbeats
longer than `broker.session.timeout.ms`, default 9 s in KRaft mode). The process doesn't have to crash.

**1. The process crashes**
- **Disk full**: the most common. Retention too long, a traffic spike, no `retention.bytes` limit.
- **Too many open files**: every segment `.log`/`.index`/`.timeindex` file and every client socket is an
  open file. Many partitions + low `ulimit -n` → crash.
- **Out of memory**: Java heap (`OutOfMemoryError`), or `Map failed` from too many memory-mapped index
  files (Linux `vm.max_map_count`).
- **Disk hardware / I/O errors.**

**2. The machine goes away**
- Hardware failure, power, VM/host reboot, cloud instance retired.
- **Kubernetes/Docker OOMKilled**: the container exceeds its memory limit.

**3. It's alive but *looks* dead**
- **Long GC pause** → misses heartbeats → controller moves its leaders away.
- **Network problem** between the broker and the controller.
- **Overloaded** (CPU, slow disk) → falls out of ISR, may miss heartbeats.
- When it repeats it's **flapping**: leaders keep moving back and forth.

**4. Planned (the most frequent in practice)**
- Rolling restarts for upgrades/config changes, pod rescheduling.
- With **controlled shutdown** (default) the broker hands its leaders to other ISR members *before*
  stopping, so clients just follow the new leader.

**Metrics to watch:**
- `UnderReplicatedPartitions` > 0 → a follower is behind or a broker is down (first warning)
- `OfflinePartitionsCount` > 0 → a partition has **no** leader, nobody can read or write it (serious)
- disk usage, GC pause time

---

## Does one dead container/pod kill all brokers?

No. Normally it's **one broker per container/pod**, each on a different machine:

```
Kubernetes:
  node A → pod kafka-0 (broker 1)
  node B → pod kafka-1 (broker 2)
  node C → pod kafka-2 (broker 3)
```

Pod `kafka-1` OOMKilled → only broker 2 dies → brokers 1 and 3 take over its partitions →
Kubernetes restarts the pod → broker 2 comes back as a follower.

**All brokers die only if they share what failed:**
- all on **one machine** (e.g. `docker-compose` on a laptop or one server)
- all pods on the **same Kubernetes node** → that's why deployments use *anti-affinity* rules
- all nodes in **one data center / availability zone** → that's why production spreads brokers
  across zones and sets `broker.rack`, so Kafka puts each partition's replicas in different zones

Replication only protects you if **the copies live in places that don't fail together**.
Replication factor 3 on one machine protects you from nothing.

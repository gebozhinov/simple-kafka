# Quiz 4: Protocol, Brokers, Consumers, ZooKeeper and KRaft (real Kafka)

Covers the new real-Kafka material in `protocol.md`, `brokers.md`, `consumers.md`, `zookeeper.md` and `kraft.md`.
Quizzes 1 and 2 covered the basics. This one goes deeper into how real Kafka works.

Answer in the chat by number (e.g. "Q7: ..."). Short answers are fine; explain *why* where it asks.
Try without looking at the notes first. ⭐ = harder.

---

## Part A: The protocol (`protocol.md`)

**Q1.** Why does every Kafka request and response start with a 4-byte size? What does TCP give you, and what does it not give you?

**Q2.** What are the four fields of the request header, and what is each one for? Why doesn't the response header
contain the API key?

**Q3.** Decode this request by hand: `00 00 00 0D 00 12 00 00 00 00 00 01 00 03 63 6C 69`.

**Q4.** What does the ApiVersions handshake do, and why does it let a 2.8 client talk to a 3.9 broker? When does it fail?

**Q5.** simple-kafka has no version field. What happens if you add a new field to its produce request while old clients
are still running?

**Q6.** What are tagged fields? What problem do they solve that versions alone don't?

**Q7.** Encode 0, -1, 1 and 64 as zig-zag varints. Why does Kafka use varints inside records?

**Q8.** What is pipelining? How does the broker keep responses in order on one connection, and what's
`max.in.flight.requests.per.connection`?

**Q9.** One Produce request contains batches for partitions 0 and 1. Partition 0 succeeds, partition 1 fails. What
does the response look like? Which errors are retriable: `NOT_LEADER_OR_FOLLOWER`, `MESSAGE_TOO_LARGE`,
`NOT_ENOUGH_REPLICAS`, `TOPIC_AUTHORIZATION_FAILED`?

**Q10.** Which request does a follower broker use to replicate? How does the leader know it's a follower and not a
consumer?

**Q11.** What are fetch sessions, and why do they matter for a consumer with 1000 partitions?

**Q12.** ⭐ The article says binary encoding enables zero-copy. What really makes zero-copy possible in Kafka? Why does
TLS prevent it?

**Q13.** What does an over-quota client experience? Is its request rejected?

---

## Part B: Inside a broker (`brokers.md`)

**Q14.** Name the thread types in a broker and what each does: acceptor, network (processor), I/O (request handler),
replica fetcher.

**Q15.** ⭐ Produce latency at work is high. The broker metrics show `RequestQueueTimeMs` is high but `LocalTimeMs` and
`RemoteTimeMs` are low. Where is the problem, and what would you change? What if `RemoteTimeMs` were high instead?

**Q16.** What is a purgatory? Give two kinds of requests that wait in one, and what each waits for.

**Q17.** Explain `listeners` vs `advertised.listeners`. Describe the classic failure where bootstrap works but producing
times out.

**Q18.** Replication in Kafka is "pull". What does that mean, and what are two advantages over simple-kafka's "push"?

**Q19.** List the five leader-election cases. Which one can lose data?

**Q20.** What is the leader epoch, and what do `FENCED_LEADER_EPOCH` and `UNKNOWN_LEADER_EPOCH` mean for a client?

**Q21.** How is the group coordinator of `group.id = "billing-service"` chosen? What happens to the group when that
broker dies?

**Q22.** Describe a broker's lifecycle from start to clean shutdown in KRaft. What does "fenced" mean?

**Q23.** Why must a rolling restart wait for `UnderReplicatedPartitions = 0` between brokers? What can go wrong with
`min.insync.replicas=2` if you don't?

**Q24.** What does `broker.rack` do? Why is RF 3 on one machine (or in one zone) not real protection?

**Q25.** Which metric tells you that `acks=all` writes are failing right now? Which one tells you a partition is
completely unavailable?

**Q26.** A Kafka pod without a persistent volume restarts. What happens to its partitions, and what does the cluster
have to do?

---

## Part C: Consumers (`consumers.md`)

**Q27.** `poll()` does more than return records. Name four other things it does.

**Q28.** What's the difference between the consumer's **position** and its **committed offset**? Where is each stored?

**Q29.** What does `max.poll.records` change, and what does it not change?

**Q30.** Explain inside-group vs between-groups delivery. How is Kafka both a queue and publish/subscribe?

**Q31.** Walk through a classic rebalance when consumer C joins A and B: which requests, who is the group leader,
and who computes the assignment.

**Q32.** What is the generation id, and how does it stop an old member's commit?

**Q33.** ⭐ `session.timeout.ms` vs `max.poll.interval.ms`: what does each detect? Describe a case where heartbeats are
fine but the consumer is still kicked out. What happens to its commit, and to the records it was processing?

**Q34.** Assign `orders` (3 partitions) and `payments` (3 partitions) to consumers A and B with Range, then with RoundRobin.

**Q35.** Eager vs cooperative rebalancing: what happens to consumption during each? Why does cooperative need two rounds?

**Q36.** What does `group.instance.id` change when a pod restarts? What's the trade-off?

**Q37.** ⭐ Name three differences between the classic protocol and the new KIP-848 protocol. Where is the assignment
computed in each?

**Q38.** What exactly does a commit write, and where? Why is `__consumer_offsets` compacted?

**Q39.** Why doesn't `commitAsync` retry? What's the common pattern combining `commitAsync` and `commitSync`?

**Q40.** A group was stopped for 10 days. What happens when it starts again? Which config decides?

**Q41.** How does consume → produce become exactly-once? What must downstream consumers set?

**Q42.** In a `ConsumerRebalanceListener`, why commit in `onPartitionsRevoked` but **not** in `onPartitionsLost`?

**Q43.** Your Spring Boot app has `concurrency = "3"` and 4 pods, and the topic has 8 partitions. How many `KafkaConsumer`
instances are there, and how many are idle? What does `AckMode.BATCH` commit, and when?

**Q44.** What does Spring's `DefaultErrorHandler` do with a record that keeps failing? What is a poison pill, and how do
you handle it?

**Q45.** In `kafka-consumer-groups.sh --describe`, what is `LOG-END-OFFSET` really? Why is lag in records sometimes misleading?

---

## Part D: ZooKeeper in depth (`zookeeper.md`)

**Q46.** What's in a znode's stat? Which field tells you which session owns an ephemeral node?

**Q47.** Name the five node types. Which ones did Kafka use, and for what?

**Q48.** How is the session timeout negotiated? Why can a client not know its session expired until it reconnects,
and why does that matter for a controller?

**Q49.** Which reads can set a watch, and which events does each catch? Why does a watch event carry no data?

**Q50.** Leader, follower, observer: what does each do? Why add observers instead of more followers?

**Q51.** What is a zxid made of? Where else in Kafka have you seen the same "epoch" idea?

**Q52.** Walk through a ZooKeeper write. Why is ZooKeeper good for metadata but bad for messages?

**Q53.** ⭐ In real ZooKeeper-mode Kafka, how did a partition leader change the ISR safely, and how was a zombie
controller stopped from writing to ZooKeeper?

**Q54.** Real Kafka kept partition **assignment** and partition **state** in different znodes. Why?

**Q55.** The article says ZooKeeper stores consumer offsets and tracks consumer group membership. Is that true today?

---

## Part E: KRaft (`kraft.md`)

**Q56.** What is the active controller in KRaft? Is it a broker? Can clients connect to it?

**Q57.** What is `__cluster_metadata`? Name five record types and what each means.

**Q58.** Write the metadata records for: broker 6 registers, topic `orders` is created with 2 partitions, then broker 4 dies
while leading `orders-0`.

**Q59.** Raft commits a record when a **majority** has it; partition replication commits when **all ISR** have it. Why
the difference?

**Q60.** ⭐ Walk through a controller election after the leader dies. Which two conditions must a voter check before
granting its vote, and why does the second guarantee that nothing committed is lost?

**Q61.** How does the controller find out a broker died, without ZooKeeper sessions? What is the broker epoch for?

**Q62.** Why are metadata snapshots needed? What does a new broker do to catch up?

**Q63.** What's in `quorum-state`, and what could go wrong without it?

**Q64.** 2 of 3 controllers are down. What still works, and what doesn't?

**Q65.** Map each ZooKeeper-mode mechanism to its KRaft replacement: ephemeral `/brokers/ids/N`, `/controller` election,
`/controller_epoch`, `UpdateMetadata` / `LeaderAndIsr`, ZooKeeper ISR write.

**Q66.** Your work cluster runs ZooKeeper mode on Kafka 3.x. Can you upgrade straight to 4.0? What must happen first?

---

## Bonus: your work cluster

**B1.** Run `kafka-broker-api-versions.sh`. Which Kafka version do the brokers run (look at the max versions)?

**B2.** What are `listeners` and `advertised.listeners` on your brokers? Which security protocol do your apps use?

**B3.** For your main consumer group: `kafka-consumer-groups.sh --describe --state`. Which assignor and protocol
(classic or consumer)? What's the lag?

**B4.** What are `max.poll.interval.ms`, `max.poll.records` and `session.timeout.ms` in your Spring config? Have you
seen `CommitFailedException` or frequent rebalances in the logs?

**B5.** ZooKeeper or KRaft? If KRaft: `kafka-metadata-quorum.sh describe --status`. Who is the leader, and how many
voters and observers are there?

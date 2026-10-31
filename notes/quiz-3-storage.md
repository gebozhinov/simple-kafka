# Quiz 3: Partitions and the Storage Layer

Covers how partitions work in **real Kafka** (`partition.md`) and stage 4 (`Partition.java`, `stage-4-storage.md`).

Answer in the chat by number (e.g. "Q7: ..."). Short answers are fine; explain *why* where it asks.
Try without looking at the notes or the code first. ⭐ = harder.

---

## Part A: The units

**Q1.** What is a partition, in one sentence? Name the three rules that define it.

**Q2.** Is Kafka a queue? What happens to a record after a consumer reads it?

**Q3.** Put these in order from biggest to smallest and say in one line what each is:
record, segment, topic, record batch, replica, partition.

**Q4.** Where is the data: in the topic or in the partitions? What is a topic, then?

**Q5.** Partition 0 and partition 1 of `orders` both have a record at offset 5. Is it the same record? Why?

**Q6.** What's the difference between a partition and a replica? What are the leader, the followers, and the
preferred leader?

**Q7.** Why is the **record batch** the most important unit for the broker? Who creates it, and does the broker
ever unpack it?

**Q8.** Real Kafka splits our one `Partition` class into several classes. What does `kafka.cluster.Partition` take
care of, and what does `UnifiedLog` take care of? What does `LogSegment` hold?

---

## Part B: Why partitions, and choosing one

**Q9.** Give three reasons why topics are split into partitions.

**Q10.** A shop must process `created → paid → shipped` for each order in the right order. How do you make sure it
happens? What is **not** guaranteed?

**Q11.** A topic has 4 partitions. Your Spring Boot app runs 3 pods, each with `@KafkaListener(concurrency = "2")`.
How many consumer threads actually get work?

**Q12.** How does the Java producer pick a partition (a) with a key, (b) without a key? Why "sticky" and not
"one record to each partition in turn"?

**Q13.** ⭐ You increase `orders` from 6 to 8 partitions. What happens to the existing data? Why can this break
ordering for a customer? Why can't you go back to 6?

---

## Part C: Offsets

**Q14.** Who assigns a record's offset: the producer, the leader, or the follower? In which step of the write path?

**Q15.** Define: log start offset, LEO, high watermark, last stable offset, committed offset. Which one is simple-kafka's
`nextOffset`? Which one is **not** stored in the partition, and where is it?

**Q16.** ⭐ Why can consumers read only below the high watermark? Describe exactly what could go wrong if they could
read up to the leader's LEO.

**Q17.** What is consumer lag, in terms of these offsets?

**Q18.** Why must a consumer never assume `next offset = previous offset + 1`? Give two reasons offsets can have gaps.

---

## Part D: Creating partitions

**Q19.** Walk through `kafka-topics.sh --create --partitions 3 --replication-factor 3` in KRaft: who receives it,
who decides the replicas, how the brokers find out, and who creates which directories.

**Q20.** Broker 4 isn't a replica of `orders-0`. Does it have an `orders-0` directory? In simple-kafka?

**Q21.** How does a replica end up on a new broker during a reassignment? Is the data copied, and by whom?

**Q22.** After a restart, is a partition "created" again? What happens to the existing files?

---

## Part E: Files on disk

**Q23.** List the files in `orders-0/` in real Kafka and say what each one is for:
`.log`, `.index`, `.timeindex`, `.snapshot`, `.txnindex`, `leader-epoch-checkpoint`, `partition.metadata`.

**Q24.** What do `recovery-point-offset-checkpoint`, `replication-offset-checkpoint` and `.kafka_cleanshutdown` store
or mean?

**Q25.** Why is a segment file named `00000000000000042000.log`? What is 42000, and why 20 digits?

**Q26.** Give three reasons why a partition's log is split into segments.

**Q27.** In the v2 record batch, which fields are in the **batch header** and which are in each **record**? Why do the
records store only *deltas*?

**Q28.** Where is the CRC, and what is it used for, on the broker and on the consumer?

**Q29.** Why does compression work better with bigger batches? Does the broker decompress batches before storing
them?

**Q30.** Write the exact bytes simple-kafka's `.log` contains after `append("hi")` then `append("kafka")`. What does
real Kafka store instead?

---

## Part F: Indexes and segments

**Q31.** What does one real `.index` entry contain, and how many bytes? Why a **relative** offset?

**Q32.** ⭐ Walk through finding offset 42080 in segment `…42000` with the index entries
`(0,0) (37,4105) (75,8230) (112,12290)`. What is scanned after the index lookup, and why is the scan never long?

**Q33.** "Sparse" vs "dense" index: which does simple-kafka have, which does real Kafka have, and what's the trade-off?
(Note: the article calls simple-kafka's index sparse. Is it?)

**Q34.** Why can a segment never be larger than 2 GB?

**Q35.** What is the `.timeindex` used for? Name three things that use it.

**Q36.** List the four conditions that roll a segment in real Kafka. What happens to the old segment's indexes on roll?

**Q37.** ⭐ A topic has `retention.ms=1h`, the default 1 GB `segment.bytes`, and only 10 MB of traffic per day. How long
does data actually stay? How would you fix it?

**Q38.** The `.index` file of a closed segment is corrupted. What do you do, and why is it safe?

---

## Part G: Write path and replication

**Q39.** Describe the real write path for `acks=all`, from `send()` to "OK". At which step is the data written
to disk, and is it fsynced?

**Q40.** ⭐ If Kafka doesn't fsync every record, why is `acks=all` data still durable? When does Kafka fsync?

**Q41.** What does `appendAsFollower` do differently from `appendAsLeader`? What goes wrong in simple-kafka because its
followers call the same `append()`?

**Q42.** How does the leader know each follower's LEO? How does a follower learn the high watermark, and why is it one
round behind?

**Q43.** When is a follower removed from the ISR (config and default)? With RF 3 and `min.insync.replicas=2`, what
happens to `acks=all` and `acks=1` writes when the ISR is {1}?

**Q44.** What is the leader epoch? When does it change, and which two problems does it solve?

**Q45.** ⭐ What does `unclean.leader.election.enable=true` give you, and what does it cost?

**Q46.** How does idempotence (producer id + sequence) prevent duplicates when the producer retries?

---

## Part H: Read path

**Q47.** What is the upper limit of a fetch for a follower, a `read_uncommitted` consumer, and a `read_committed`
consumer?

**Q48.** ⭐ The consumer asks for offset 505 and gets the batch 500..509. Why, and who drops 500..504?

**Q49.** What is zero-copy? Which Java call and which OS call does Kafka use? When does it **not** work?

**Q50.** Name five reasons Kafka is fast even though it writes everything to disk. Why do brokers need a small heap
but lots of RAM?

**Q51.** What does "long poll" (`fetch.max.wait.ms`) mean?

---

## Part I: Retention, compaction, recovery

**Q52.** `retention.ms` = 7 days. Why can a 9-day-old record still be on disk? Which segment is never deleted?

**Q53.** What happens step by step when a segment is deleted by retention? Why is it renamed to `.deleted` first?

**Q54.** A consumer group's committed offset is 100, but the log start offset is now 42000. What happens on the next
fetch with `auto.offset.reset` = `earliest`, `latest`, `none`?

**Q55.** Show the result of compacting: `0:A=1, 1:B=1, 2:A=2, 3:C=1, 4:B=null, 5:A=3`. Do offsets change? What is a
tombstone, and how long does it stay?

**Q56.** Give one topic that should use `delete` and one that should use `compact`. Which internal topic is compacted?

**Q57.** ⭐ The broker is killed with `kill -9` while writing. Describe what happens at the next startup. Why is
the restart much faster after a clean shutdown?

**Q58.** ⭐ Walk through the leader-epoch truncation example: A (epoch 5) has 0..9, B has 0..7, A dies, B leads epoch 6
and writes 8'..12'. What does A do when it comes back? Which producers lost data?

---

## Part J: simple-kafka's `Partition` methods

**Q59.** In `initialize()`, why must `segments` be sorted? How does it compute `nextOffset`? How does real Kafka get the LEO
instead?

**Q60.** What would happen if `openSegmentForAppend` didn't call `position(activeLogChannel.size())`?

**Q61.** Walk through `append("bulgaria")` when the partition has `hello, world, kafka`: the offset, the position,
the index entry and the new `nextOffset`.

**Q62.** Why does `readMessages` take the read lock and `append` the write lock? How does real Kafka read without
any lock?

**Q63.** `findSegmentForOffset(35)` with segments `[0, 14, 30, 47]`: show the binary search steps. What one call does
real Kafka use?

**Q64.** ⭐ A fetch starts at offset 3, and segments are `[0..13]` and `[14..]`. What does `readMessages` return, what is
logged, and why?

**Q65.** ⭐ A consumer fetches with `maxBytes = 1000` and the next record is 100 KB. What happens in simple-kafka, and
in real Kafka?

**Q66.** ⭐ The broker crashes in the middle of writing a 100 KB message. What is `nextOffset` after the restart,
and what does a consumer at that offset experience? What would real Kafka do with that segment?

---

## Bonus: your work cluster

**B1.** Run `kafka-topics.sh --describe` on one of your topics. How many partitions, what RF, and is every ISR complete?
Is every leader the preferred (first) replica?

**B2.** What's `log.dirs` on your brokers? List one partition directory. How many segments are there, how big are they,
and which files do you see?

**B3.** Run `kafka-dump-log.sh --print-data-log` on a small segment. How many records are in a batch?
Which compression codec is used?

**B4.** What are `retention.ms`, `segment.bytes` and `cleanup.policy` for your main topic? Broker default or topic override?

**B5.** What are `min.insync.replicas` and your producers' `acks`? What happens to your app if one broker is down?
And two?

**B6.** Compare `@KafkaListener` concurrency × number of pods with the partition count. Are any threads idle?

# Quiz 1: Protocol, Producers and Consumers

Covers stage 2 (`Protocol.java`) and what we discussed around it: bytes on the wire, the request/response flow,
offsets, commits, delivery guarantees. Related notes: `stage-2-protocol.md`, `consumers.md`.

Answer in the chat by number (e.g. "Q7: ..."). Short answers are fine; explain *why* where it asks.
Try without looking at the notes or the code first. ⭐ = harder.

---

## Part A: Bytes and `ByteBuffer`

**Q1.** Why does the protocol send bytes instead of just sending a Java object or a String like `"PRODUCE orders 0 hi"`?

**Q2.** What do `position`, `limit` and `capacity` mean in a `ByteBuffer`? What exactly does `flip()` change?

**Q3.** You remove `buffer.flip()` from `encodeProduceRequest` and call `socketChannel.write(buffer)`. What happens? Is there an exception?

**Q4.** How many bytes does `ByteBuffer.allocate(...)` need for `encodeCreateTopicRequest("payments", 6, (short) 3)`? Show the calculation.

**Q5.** Why is `topic.length()` wrong for calculating the buffer size, while `topic.getBytes(StandardCharsets.UTF_8).length` is right? Give an example topic name where they differ.

**Q6.** ⭐ This compiles. What goes wrong at runtime, and why does Java allow it?
```java
ByteBuffer buffer = ByteBuffer.allocate(3 + msgBytes.length);
buffer.putInt(ERROR_RESPONSE);
buffer.putShort((short) msgBytes.length);
buffer.put(msgBytes);
```

**Q7.** In `decodeFetchResponse` the offset is read with `buffer.getLong()` but never used. Why can't you just delete that line?

---

## Part B: Reading the wire (decode these by hand)

**Q8.** What request is this? Write down every field and its value.
```
01 00 03 66 6F 6F 00 00 00 02 00 00 00 03 61 62 63
```
(hint: `66 6F 6F` = "foo", `61 62 63` = "abc")

**Q9.** The consumer receives this. How many messages, at which offsets, with what content? Which offset should the **next** fetch use?
```
12 00 00 00 01 00 00 00 00 00 00 00 0A 00 00 00 02 6F 6B
```
(hint: `6F 6B` = "ok")

**Q10.** The producer receives this. Did it work? What's the result of `decodeProduceResponse`?
```
11 00 00 00 00 00 00 00 2A 00
```

**Q11.** The consumer receives `12 00 00 00 00`. Is this an error? What does it mean, and what should the consumer do?

---

## Part C: The request/response flow

**Q12.** Describe the full flow of a producer sending "hi" and a consumer reading it later. For each step say:
**who** calls **which** method (`encode...` / `decode...`), and **who** builds the answer.

**Q13.** Does the broker call `decodeFetchResponse`? Are the bytes that go into `decodeFetchResponse` the same bytes that
came out of `encodeFetchRequest`? Explain.

**Q14.** The consumer calls `encodeFetchRequest`. Where exactly does it **wait** for the broker's answer, and what happens
if the broker never answers?

**Q15.** ⭐ A real Kafka broker gets a fetch request but has no new messages. What does it do instead of answering
"0 messages" right away, and which two consumer settings control it? Why is that better than simple-kafka's way?

**Q16.** Why does `encodeProduceRequest` have **no** offset field, but `encodeReplicateRequest` **does**?

**Q17.** Why does `encodeMetadataRequest()` need only 1 byte? What does the client do with the answer?

**Q18.** Why is the **first byte** of every response read before anything else in the decoders?

**Q19.** `byte[][] messages = new byte[3][];` What exists in memory right after this line? Where do the inner arrays get their size?
Why isn't it `new byte[3][100]`?

**Q20.** `FetchResult` is a record holding a `byte[][]`, `MetadataResult` is a record holding `List`s. Why does `equals()`
behave differently for them?

**Q21.** Kafka uses its own binary protocol, not JSON over HTTP. But JSON is also bytes on the wire. So what's actually
different? Give **three** advantages of Kafka's binary format and **one** disadvantage.

**Q22.** Your messages at work may be JSON. Does that mean Kafka uses JSON? Explain the two layers.

---

## Part D: Producers

**Q23.** Why does the producer get an offset back at all? Give two practical uses.

**Q24.** In `decodeProduceResponse`, what's the difference between getting an `ERROR_RESPONSE` and getting a
`PRODUCE_RESPONSE` with status `1`?

**Q25.** ⭐ The broker stores the message, then crashes before the response reaches the producer. What does the producer
see? If it retries, what can go wrong, and what does real Kafka use to prevent it?

**Q26.** With `acks=0`, what does the producer know about its message?

---

## Part E: Consumers and offsets

**Q27.** The broker doesn't remember where each consumer stopped reading. So how does a consumer read the next messages,
and how can it re-read old ones? Why can't a traditional queue do that?

**Q28.** ⭐ The reference consumer computes its next offset as `currentOffset += messages.size()`. Why is that wrong in
real Kafka? What does real Kafka use instead?

**Q29.** A message of 2 MB sits at offset 42, the consumer fetches with `maxBytes = 1 MB`. What happens in simple-kafka?
What does real Kafka do since KIP-74? Where else does real Kafka limit message size?

**Q30.** A consumer restarts. In simple-kafka, where does it continue? In real Kafka, how does it know where to continue,
and where is that stored?

**Q31.** What does `auto.offset.reset` do and when is it used? `earliest` vs `latest`?

**Q32.** Commit **before** processing vs commit **after** processing: which is at-least-once, which is at-most-once?
What does each one lose or duplicate on a crash?

**Q33.** ⭐ `enable.auto.commit=true`, and the consumer hands each message to a thread pool and goes back to `poll()`
immediately. Why can this **lose** messages?

**Q34.** At-least-once means duplicates will happen. What should the processing code be, so duplicates don't hurt?

**Q35.** A consumer group was stopped for 10 days. What can go wrong when it starts again?

**Q36.** In a consumer group with a topic of 3 partitions, you start 5 consumers. What do consumers 4 and 5 do?

---

## Bonus: your work

**B1.** Pick one producer at work: what are its `acks` and `enable.idempotence`? Is it safe against a broker restart?

**B2.** Pick one consumer at work: auto-commit or manual (or Spring's ack mode)? Same thread or a thread pool?
Is it at-least-once or can it lose messages?

# Quiz 2: Brokers, Cluster and ZooKeeper

Covers how a Kafka cluster works (brokers, leaders, replicas, failures) and stage 3 (`ZookeeperClient.java`).
Related notes: `brokers.md`, `zookeeper.md`, `stage-3-zookeeper.md`.

Answer in the chat by number (e.g. "Q7: ..."). Short answers are fine; explain *why* where it asks.
Try without looking at the notes or the code first. ⭐ = harder.

---

## Part A: Brokers and clients

**Q1.** What is a broker? What is a client? Is your Spring Boot app in its pod the client, or is the pod the client?
Name three kinds of clients.

**Q2.** Can a broker be a client? When?

**Q3.** A producer knows only `bootstrap.servers=kafka-0:9092`. How does it find out which broker to send
`orders` partition 2 to?

**Q4.** ⭐ The producer connects to the bootstrap broker fine, gets metadata, then times out connecting to the leader.
What's a very common cause?

---

## Part B: Leaders and replicas

**Q5.** "Broker 2 is the leader broker." Why is this sentence misleading?

**Q6.** Why does each partition have only **one** leader? What would go wrong with two?

**Q7.** Topic `orders`, 3 partitions, replication factor 2, 3 brokers. Draw a possible placement of leaders and followers.
Then broker 2 crashes: which partitions are affected, what happens to them, and what do producers notice?

**Q8.** What's the ISR? When is a follower removed from it?

**Q9.** A producer uses `acks=1`. Describe how a message can be **lost** even though the producer got an OK.

**Q10.** Why do production clusters use replication factor 3 + `min.insync.replicas=2` + `acks=all`?
What happens when only 1 replica is in sync?

**Q11.** All ISR replicas of a partition are dead. What are Kafka's two options, and which one is the default? Why?

**Q12.** Simple-kafka pushes data to followers (`REPLICATE`). Real Kafka followers pull (Fetch). Give two reasons why pull is better.

**Q13.** You created a topic with 12 partitions and now want 6. What are your options and what does each cost?
What's the risk of going from 12 to 24 instead?

---

## Part C: When brokers die

**Q14.** Name four different causes for a broker to "die". Which one doesn't involve the broker process crashing at all?

**Q15.** What does "the controller considers a broker dead" actually mean? Why can a healthy broker be considered dead?

**Q16.** Replication factor 3, but all 3 broker pods run on the same Kubernetes node. Are you protected? Why or why not?
What do production setups do about it?

**Q17.** Which two broker metrics would you watch, and what does each one mean when it's above 0?

**Q18.** Your cluster has only **one** broker pod. List three consequences.

---

## Part D: ZooKeeper basics

**Q19.** What is ZooKeeper used for in simple-kafka? Name three things. Do producers and consumers talk to it?

**Q20.** "ZooKeeper helps brokers talk to each other." Is that right? What's the whiteboard vs phone call difference?

**Q21.** Persistent vs ephemeral node: what's the difference? When is an ephemeral node deleted, and by whom?

**Q22.** Which nodes in simple-kafka are persistent and which are ephemeral? Why can't `/brokers` itself be ephemeral?
What's special about `/controller` at startup?

**Q23.** What are the "children" of `/topics/orders`? What does `getChildren("/brokers")` return: names or full paths?

**Q24.** Why is a ZooKeeper ensemble always an odd number of servers (3, 5)? How many can a 5-server ensemble lose?

---

## Part E: The flows

**Q25.** Broker 3 crashes (kill -9). Step by step: what happens in ZooKeeper, and how do the other brokers find out?
How long does it take? What if broker 3 had shut down cleanly with `close()` instead?

**Q26.** A new topic is created. Describe the whole flow: what the controller writes to ZooKeeper, what `TOPIC_NOTIFICATION`
contains, and what each other broker does when it receives it (`loadTopic`). Why does the notification contain only the name?

**Q27.** After loading `orders`, broker 2 is the leader of partition 1, a follower of partition 0, and not a replica of
partition 2. What does it do with a produce request for each of the three?

**Q28.** What is the controller and what does it decide? If there's no controller for a while, what still works and what doesn't?

**Q29.** How is the controller chosen in simple-kafka? At which moments does the election run?
Does the controller move back when the old controller returns?

**Q30.** How is the controller chosen in KRaft? What's different from "first one wins"?

---

## Part F: `ZookeeperClient` code

**Q31.** In `connect()`, why is `connectedSignal.await()` needed after `new ZooKeeper(...)`? Who calls `countDown()`?

**Q32.** Who calls `process(WatchedEvent)`, and why is it called at all? What's the difference between `Disconnected` and `Expired`?

**Q33.** ⭐ Why was this version of `createEphemeralNode` a bug, and why does the fixed version (just `create` + catch
`NodeExistsException`) work? What's the general rule?
```java
if (zooKeeper.exists(path, false) == null) {
    zooKeeper.create(path, ..., CreateMode.EPHEMERAL);
    return true;
}
return false;
```

**Q34.** `watchChildren("/brokers")` vs `watchNode("/controller")`: what does each one react to? Which one would notice
`setData("/brokers/2", "newhost:9092")`?

**Q35.** Watches are "one-time". What does that mean, and how does each of the two watch methods deal with it?

**Q36.** Who calls `watchChildren`, `watchNode` and `deleteNode` in the broker, and why? Why does the broker delete
`/controller` before the election?

**Q37.** What does the `-1` in `setData(path, data, -1)` mean? What would passing the real version give you?

**Q38.** ⭐ After a session expires and the client reconnects, why is the broker "invisible" unless the new-session callback
runs? What must the callback do, and why `isController.set(false)` first?

**Q39.** What is a "zombie controller"? How does real Kafka prevent it?

**Q40.** Why did Kafka replace ZooKeeper with KRaft? Give two reasons.

---

## Bonus: your work cluster

**B1.** How many brokers does your work cluster have? How did you check?

**B2.** Is it ZooKeeper or KRaft? Which config property tells you?

**B3.** Does the Kafka pod have a persistent volume? What happens to the messages and committed offsets if it doesn't
and the pod restarts?

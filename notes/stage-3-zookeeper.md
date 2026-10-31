# Stage 3 – ZooKeeper Integration Notes

Source: https://buildthingsuseful.medium.com/stage-3-build-your-own-kafka-3baaf4c873de
Code: `src/main/java/com/simplekafka/broker/ZookeeperClient.java`
ZooKeeper concepts (znodes, ephemeral nodes, sessions, watches): see `zookeeper.md`
How Kafka does the same today without ZooKeeper (KRaft): see `kraft.md`

`ZookeeperClient` is a thin wrapper around the official ZooKeeper Java library (`org.apache.zookeeper.ZooKeeper`).
The broker calls these methods instead of using the library directly.

All examples below were run against a real ZooKeeper 3.8 (Docker) with this code.

---

## Fields and constructor

```java
private static final int SESSION_TIMEOUT = 30000;              // 30 s
private ZooKeeper zooKeeper;                                   // the library's connection object
private CountDownLatch connectedSignal = new CountDownLatch(1);// "wait until connected" gate

public ZookeeperClient(String host, int port)                  // just stores host and port, doesn't connect
```

The class `implements Watcher`, so it has a `process(WatchedEvent)` method that ZooKeeper calls for
connection events (see `process()` below).

## `getConnectString()`

Returns `host + ":" + port` → `"localhost:2181"`. That's the format the ZooKeeper library expects.
With an ensemble of several servers it would be `"zk1:2181,zk2:2181,zk3:2181"`, and the library picks one,
moving to another if it fails.

---

## `connect`

### Purpose

Opens a session with ZooKeeper and makes sure the base folders exist. Every other method needs this first.
Each broker calls it once at startup.

### Line by line

```java
zooKeeper =new

ZooKeeper(getConnectString(),SESSION_TIMEOUT, this);
```

- `SESSION_TIMEOUT` → if ZooKeeper hears nothing from this broker for 30 s, the session expires and its
  ephemeral nodes are deleted.
- `this` → the default watcher: ZooKeeper will call `this.process(event)` for connection events.

**The constructor doesn't wait for the connection.** It returns immediately and connects in a background thread.

```java
connectedSignal.await();
```

`CountDownLatch(1)` is a gate that starts closed. `await()` stops this thread until `countDown()` opens it.
`process()` calls `countDown()` when the connection is ready:

```
main thread                          ZooKeeper background thread
new ZooKeeper(...)  → returns
connectedSignal.await()  ⏸ waiting   ... connecting to localhost:2181 ...
                                     connected → process(SyncConnected)
                                                 → connectedSignal.countDown()
  ▶ continues
createPath("/brokers") ...
```

Without the gate, `createPath` would run before the connection exists and fail with `ConnectionLossException`.

```java
createPath("/brokers");

createPath("/topics");

createPath("/controller");
```

Result (tested): `getChildren("/")` → `[controller, brokers, zookeeper, topics]`
(`/zookeeper` is ZooKeeper's own internal node, always there.)

### Pitfalls

- `await()` has no timeout. If ZooKeeper is down, the constructor doesn't fail (it retries in the background),
  so the broker hangs at startup with no error. `connectedSignal.await(10, TimeUnit.SECONDS)` returns `false`
  on timeout, so you can throw a clear exception.
- **`createPath("/controller")` creates a *persistent*, empty `/controller`.** But the controller election wants
  `/controller` to be an *ephemeral* node owned by the winner. That's why the reference broker's `electController()`
  first checks "does `/controller` exist with empty data? then delete it" before trying to create it.
  It would be cleaner not to create `/controller` here at all.

---

## `createPath(String path)` (private)

### Purpose

Creates a path **and all its parents**, like `mkdir -p`. ZooKeeper can only create a node if its parent exists.

### Example

```
createPath("/brokers/ids")
  lastSlashIndex = 8 → parent = "/brokers"
  createPath("/brokers")
     lastSlashIndex = 0 → directly under "/", no parent to create
     exists("/brokers")? no → create "/brokers"
  exists("/brokers/ids")? no → create "/brokers/ids"
```

Calling it again does nothing. Important, because **every** broker calls `connect()` on startup.

`zooKeeper.create(path, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)`:

- `new byte[0]` → no data, just a folder
- `OPEN_ACL_UNSAFE` → anyone can read/write/delete (no security; fine for learning)
- `PERSISTENT` → stays after this broker disconnects

### Pitfalls

- **Two brokers starting at once:** both see "doesn't exist", both create, the second gets `NodeExistsException`.
  It's caught and logged as a warning: harmless, but looks alarming in logs.
- `catch (Exception e)` only logs, so a real failure is swallowed and shows up later in a confusing place.

---

## `close()`

### Purpose

Ends the session cleanly. ZooKeeper **immediately** deletes this client's ephemeral nodes and other clients'
watches fire, without waiting for the 30 s timeout.

### Example (tested)

```
before:  /brokers = [1, 2]        broker 1 holds /brokers/1, broker 2 holds /brokers/2
b1.close()
after:   /brokers = [2]           b2's watch callback fired: "brokers now [2]"
```

That's the difference between a **clean shutdown** (other brokers know instantly) and a **crash**
(they find out only after the session timeout).

---

## `createPersistentNode(String path, String data)`

### Purpose

"Make sure this node exists **with this data**." If it doesn't exist, create it. If it exists, overwrite its data.
The broker uses it for topic and partition metadata.

### Example (tested)

```java
createPersistentNode("/topics/orders","3");   // log: Created persistent node

createPersistentNode("/topics/orders","5");   // log: Updated persistent node

getData("/topics/orders")                       // → "5"
```

In the reference broker, creating topic `orders` with 3 partitions, replication factor 2:

```
createPersistentNode("/topics/orders", "")
createPersistentNode("/topics/orders/partitions", "")
createPersistentNode("/topics/orders/partitions/0", "1;2,")    leader 1; followers 2
createPersistentNode("/topics/orders/partitions/1", "2;3,")
createPersistentNode("/topics/orders/partitions/2", "3;1,")
```

### Pitfalls

- The parent must exist: `createPersistentNode("/a/b/c", ...)` fails with `NoNodeException` if `/a/b` doesn't exist.
  (That's why the broker creates `/topics/orders` and `/topics/orders/partitions` first.)
- Check-then-act race: two callers both see "doesn't exist" → the second `create` throws `NodeExistsException`.
- `data.getBytes()` uses the platform charset; `getBytes(StandardCharsets.UTF_8)` is safer.

---

## `createEphemeralNode(String path, String data)` → `boolean`

### Purpose

Creates a node that **disappears automatically when this broker's session ends**. Returns `true` if this call
created it, `false` if it already existed. Two uses in the broker:

1. **"I'm alive"**: `createEphemeralNode("/brokers/1", "localhost:9091")`
2. **Controller election**: `createEphemeralNode("/controller", "1")`. `true` = I won, I'm the controller.

### Example (tested)

```
b1.createEphemeralNode("/brokers/1", "localhost:9091")   → true
b2.createEphemeralNode("/brokers/1", "y")                → false   (log: already exists)
b1.close()                                               → /brokers/1 is gone
```

### Race between brokers (fixed)

The first version asked `exists(path)` and then called `create(path)`. Between those two steps another broker
could create the node:

```
broker 1: exists("/controller")? → no
broker 2: exists("/controller")? → no
broker 1: create("/controller")  → ✓ returns true
broker 2: create("/controller")  → ✗ NodeExistsException (never returned false)
```

Tested with the old version: 2 clients at the same instant, 20 rounds → **20/20 times the loser threw
`NodeExistsException`** instead of returning `false`. That's exactly the controller-election situation, where
everyone tries at once.

The fix: no `exists()` first. Just try to create, and treat "already exists" as the normal "you lost" answer.
ZooKeeper's `create` is atomic, so it already does the check:

```java
try {
    zooKeeper.create(path, data.getBytes(StandardCharsets.UTF_8), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
    return true;
} catch (KeeperException.NodeExistsException e) {
    return false;
}
```

Tested after the fix: 3 clients at the same instant, 20 rounds → **20/20 rounds: exactly one `true`, two `false`,
no exceptions**. Example round: broker1=false, broker2=false, broker3=true, `/race` data = "3".

General rule: with shared state, **"check, then act" is a race**. Let the atomic operation be the check.

---

## `exists(String path)` → `boolean`

### Purpose

Is there a node at this path? `zooKeeper.exists` returns a `Stat` (the node's metadata: version, creation time,
owner session...) or `null`. This method turns that into `true`/`false`.

`false` as the second argument = don't set a watch.

### Example

```
exists("/brokers")     → true
exists("/brokers/9")   → false
```

Note: the answer can be outdated the moment you get it (another broker may create/delete the node right after).
Don't build "check, then act" logic on it when other brokers act at the same time (see the race above).

---

## `getData(String path)` → `String`

### Purpose

Reads the data stored in a node.

### Example (tested)

```
getData("/brokers/1")                  → "localhost:9091"
getData("/topics/orders/partitions/0") → "1;2,"      (the broker then parses leader 1, followers [2])
getData("/nope")                       → NoNodeException
```

`getData(path, false, null)`: `false` = no watch, `null` = don't return the `Stat`.

### Pitfalls

- Throws `NoNodeException` if the node doesn't exist (or was just deleted, e.g. a broker that died).
- `new String(data)` uses the platform charset; use `StandardCharsets.UTF_8`.
- A node created with `new byte[0]` returns `""`.

---

## `setData(String path, String data)`

### Purpose

Overwrites the data of an **existing** node (`NoNodeException` if it doesn't exist).

### Example

```
setData("/topics/orders/partitions/1", "3;2,")    leader of partition 1 moved to broker 3
```

### The `-1`: versions

Every znode has a **version** that increases with each change. `setData(path, data, 5)` means
"only if the version is still 5", otherwise `BadVersionException`. That's **optimistic locking**:

```
controller A reads partition 1 (version 5), decides leader = 3
controller B reads partition 1 (version 5), decides leader = 1
A: setData(..., "3;2,", 5) → ✓ version becomes 6
B: setData(..., "1;2,", 5) → ✗ BadVersionException: someone changed it, re-read and decide again
```

`-1` means "any version": the last writer silently wins. Simple, but two writers can overwrite each other.

---

## `getChildren(String path)` → `List<String>`

### Purpose

Lists the names of a node's children.

### Example (tested)

```
getChildren("/brokers")  → [1, 2]           names only, not "/brokers/1"
getChildren("/nope")     → []               NoNodeException is turned into an empty list
```

The broker uses it to list all brokers, all topics, and all partitions of a topic. To read a child's data,
build the full path: `getData("/brokers/" + id)`.

Note: the order of children isn't guaranteed. Sort them if order matters.

---

## `watchChildren(String path, ChildrenCallback callback)`

### Purpose

"Tell me **every time** a child is added or removed under this path, and give me the current list."
The broker uses it on `/brokers` to know when brokers join or die.

### Example (tested)

```java
watchChildren("/brokers",ids ->System.out.

println("brokers now "+ids));
```

```
callback: brokers now []           ← called once immediately with the current list
b1 creates /brokers/1  → callback: brokers now [1]
b2 creates /brokers/2  → callback: brokers now [1, 2]
b1 creates /brokers/3  → callback: brokers now [1, 2, 3]
b1.close()             → callback: brokers now [2]       (1 and 3 were b1's ephemeral nodes)
```

### How it works

ZooKeeper watches are **one-time**: after firing once, they're gone. So the code re-registers a watch every time:

```
getChildren(path, watcherA)              → callback(current list)
  change #1 → watcherA fires
     getChildren(path, watcherB)         → callback(new list)       (watcherB is now armed)
  change #2 → watcherB fires
     watchChildren(path, callback)       → starts over with a new watcherA, callback(new list)
  change #3 → watcherA fires → ... and so on
```

It works, but the two nested lambdas do the same job. A simpler version: one watcher that, when it fires,
just calls `watchChildren(path, callback)` again.

### Pitfalls

- The callback runs on ZooKeeper's single **event thread**. A slow callback delays every other ZooKeeper event.
- If the watched node itself is deleted, the event type is `NodeDeleted`, which isn't handled → watching stops.
- At shutdown you'll see `SEVERE: Error processing children changed event`: the last event arrives while the
  connection is closing, and re-reading fails. Harmless.
- **After session expiry the watch is gone** (see `process()`).

---

## `watchNode(String path, NodeCallback callback)`

### Purpose

"Tell me when **this node** is created, deleted, or its data changes." The broker uses it on `/controller`:
when the controller dies, its ephemeral node is deleted, and the other brokers start a new election.

### It fires only once (tested)

```
watchNode("/topics/orders", ...)
setData("/topics/orders", "6")   → node callback fired
setData("/topics/orders", "7")   → (nothing)
```

Unlike `watchChildren`, this one does **not** re-register itself. The caller has to call `watchNode` again.
The reference broker does that indirectly:

```
watchNode("/controller", onControllerChange)
  /controller deleted → onControllerChange() → electController()
     → lost the election → watchNode("/controller", onControllerChange)   ← re-armed here
```

Forget this, and only the **first** controller failure is noticed.

`exists(path, watcher)` is used (not `getData`) because `exists` can watch a node that **doesn't exist yet**
and fires `NodeCreated` when it appears.

The callback gets no information about what happened. It's just "something changed, go look".

---

## `deleteNode(String path)`

### Purpose

Deletes a node if it exists.

### Example (tested)

```
deleteNode("/race")      → deleted
deleteNode("/topics")    → NotEmptyException     (it has children: delete them first, bottom-up)
```

`delete(path, -1)`: `-1` = any version (same idea as `setData`).

### Pitfalls

- Check-then-act race again: another broker can delete it between `exists` and `delete` → `NoNodeException`.
- Can't delete a node that has children.
- In the reference, used to delete a leftover empty `/controller` before the election (see `connect()` pitfalls).

---

## `process(WatchedEvent event)`

### Purpose

The default watcher: ZooKeeper calls it for **connection state changes** of this session.

| State           | What it means                                                                     | What the code does                                                       |
|-----------------|-----------------------------------------------------------------------------------|--------------------------------------------------------------------------|
| `SyncConnected` | connected (first time, or reconnected within the same session)                    | `countDown()` → opens the gate in `connect()` / `reconnect()`            |
| `Disconnected`  | connection lost; the session may still be alive, the library reconnects by itself | logs a warning                                                           |
| `Expired`       | the server ended the session; **ephemeral nodes and watches are gone**            | `reconnect()` → `createBasePaths()` → calls the **new session callback** |

Calling `countDown()` again on reconnect is harmless: the count is already 0.

### Session expiry: the bug and the fix (tested)

**The bug (first version).** ZooKeeper was frozen for 40 s (longer than the 30 s timeout), then resumed:

```
before:   /brokers = [1]                   broker 1 registered, watching /brokers
          Disconnected → session expired → "Reconnected to ZooKeeper after session expiry"
after:    /brokers = []                    ← broker 1 is NOT registered anymore
          broker 2 joins                   ← broker 1's watch did NOT fire
```

A new session starts **empty**: no `/brokers/1`, no `/controller` (if this broker was the controller), no watches.
The broker keeps running and serving requests, but the cluster thinks it's dead, and it no longer hears about
brokers joining or leaving. `ZookeeperClient` can reconnect, but it can't know what the broker had registered.

**The fix: a new session callback.** `ZookeeperClient` reconnects, then tells the broker "you have a new, empty
session", and the broker redoes its setup:

```java
// ZookeeperClient
public void setNewSessionCallback(NewSessionCallback callback)

// in process(), on Expired:
if (reconnect()) {                      // keeps retrying until a new session is connected
    createBasePaths();                  // /brokers, /topics, /controller
    newSessionCallback.onNewSession();  // the broker restores its state
}
```

What the broker will do in the callback (stage with `SimpleKafkaBroker`):
```java
zkClient.setNewSessionCallback(() -> {
    isController.set(false);                                     // I'm not the controller anymore (my /controller is gone)
    zkClient.createEphemeralNode("/brokers/" + brokerId, host + ":" + port);   // "I'm alive" again
    zkClient.watchChildren("/brokers", this::onBrokersChanged);  // watches are gone, set them again
    electController();                                           // take part in the election again
});
```
`isController.set(false)` first is important: it stops the **zombie controller** (a broker that still believes it's
the controller after losing its session). If it really should be controller, it wins `electController()` again.

**After the fix**, same 40 s freeze:
```
19:54:32  Disconnected from ZooKeeper
19:54:52  ZooKeeper session expired, reconnecting...
          Reconnected to ZooKeeper after session expiry, restoring state
          >> new session callback: re-registering broker 1
          Created ephemeral node: /brokers/1
after:    /brokers = [1]                   ← registered again
          broker 2 joins → b1 watch callback: brokers now [1, 2]   ← watch works again
```

### `reconnect()`: retry instead of waiting forever
The first version did `connectedSignal.await()` with no timeout: if ZooKeeper stayed down, the event thread
waited forever. Now each attempt waits up to `SESSION_TIMEOUT`, and on timeout or error it closes the half-open
client and tries again (with a 1 s pause after an error). It stops only if the thread is interrupted.

`zooKeeper` and `connectedSignal` are `volatile`: they're replaced on ZooKeeper's event thread, and other threads
(the broker's request handlers) must see the new objects, not a cached old one.

This is how real Kafka handled it in ZooKeeper mode: on every new session, the broker re-registered
`/brokers/ids/<id>` and the controller election ran again.

---

## `ChildrenCallback`, `NodeCallback` and `NewSessionCallback`

Small interfaces so the broker can pass a method reference or lambda as the callback:

```java
zkClient.watchChildren("/brokers", this::onBrokersChanged);    // void onBrokersChanged(List<String> ids)
zkClient.watchNode("/controller", this::onControllerChange);   // void onControllerChange()
zkClient.setNewSessionCallback(this::onNewZookeeperSession);   // void onNewZookeeperSession()
```

They work like `Consumer<List<String>>` / `Runnable`; named interfaces make the purpose clearer.

---

## Summary: which method the broker uses for what

| Broker task               | Methods                                                                                                      |
|---------------------------|--------------------------------------------------------------------------------------------------------------|
| start up                  | `connect()`                                                                                                  |
| register "I'm alive"      | `createEphemeralNode("/brokers/<id>", "host:port")`                                                          |
| learn about other brokers | `watchChildren("/brokers", ...)`, `getData("/brokers/<id>")`                                                 |
| elect a controller        | `exists`, `getData`, `deleteNode`, `createEphemeralNode("/controller", id)`, `watchNode("/controller", ...)` |
| store a new topic         | `createPersistentNode("/topics/<t>/partitions/<p>", "leader;followers,")`                                    |
| load topics               | `getChildren("/topics")`, `getChildren(".../partitions")`, `getData(...)`                                    |
| move a partition leader   | `setData(".../partitions/<p>", ...)`                                                                         |
| shut down                 | `close()`                                                                                                    |
| recover after session expiry | `setNewSessionCallback(...)` → re-register, re-watch, `isController = false`, re-elect |

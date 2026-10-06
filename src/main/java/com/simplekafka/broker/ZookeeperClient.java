package com.simplekafka.broker;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

/**
 * Client for interacting with ZooKeeper
 */
public class ZookeeperClient implements Watcher {
    private static final Logger LOGGER = Logger.getLogger(ZookeeperClient.class.getName());
    private static final int SESSION_TIMEOUT = 30000;

    private final String host;
    private final int port;
    // volatile: replaced on the ZooKeeper event thread after session expiry, read by other threads
    private volatile ZooKeeper zooKeeper;
    private volatile CountDownLatch connectedSignal = new CountDownLatch(1);
    private volatile NewSessionCallback newSessionCallback = () -> {};

    public ZookeeperClient(String host, int port) {
        this.host = host;
        this.port = port;
    }

    /**
     * Connect to ZooKeeper
     */
    public void connect() throws IOException, InterruptedException {
        zooKeeper = new ZooKeeper(getConnectString(), SESSION_TIMEOUT, this);
        connectedSignal.await();

        createBasePaths();
    }

    /**
     * Register code to run after an expired session was replaced by a new one.
     * The new session starts empty: the old session's ephemeral nodes and watches are gone,
     * so the callback must re-create them (re-register the broker, re-watch, re-elect the controller).
     */
    public void setNewSessionCallback(NewSessionCallback callback) {
        this.newSessionCallback = callback;
    }

    /**
     * Create required paths if they don't exist
     */
    private void createBasePaths() {
        createPath("/brokers");
        createPath("/topics");
        createPath("/controller");
    }

    /**
     * Get connection string
     */
    public String getConnectString() {
        return host + ":" + port;
    }

    /**
     * Close connection
     */
    public void close() throws InterruptedException {
        if (zooKeeper != null) {
            zooKeeper.close();
        }
    }

    /**
     * Create a persistent node
     */
    public void createPersistentNode(String path, String data) throws KeeperException, InterruptedException {
        Stat stat = zooKeeper.exists(path, false);
        if (stat == null) {
            zooKeeper.create(path, data.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            LOGGER.info("Created persistent node: " + path);
        } else {
            zooKeeper.setData(path, data.getBytes(), -1);
            LOGGER.info("Updated persistent node: " + path);
        }
    }

    /**
     * Create an ephemeral node.
     * Returns true if this call created it, false if it already exists.
     * No exists() check first: create() is atomic, so when several brokers race
     * (e.g. controller election) exactly one gets true and the others get false.
     */
    public boolean createEphemeralNode(String path, String data) throws KeeperException, InterruptedException {
        try {
            zooKeeper.create(path, data.getBytes(StandardCharsets.UTF_8), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            LOGGER.info("Created ephemeral node: " + path);
            return true;
        } catch (KeeperException.NodeExistsException e) {
            LOGGER.info("Ephemeral node already exists: " + path);
            return false;
        }
    }

    /**
     * Check if path exists
     */
    public boolean exists(String path) throws KeeperException, InterruptedException {
        Stat stat = zooKeeper.exists(path, false);
        return stat != null;
    }

    /**
     * Get data from a node
     */
    public String getData(String path) throws KeeperException, InterruptedException {
        byte[] data = zooKeeper.getData(path, false, null);
        return new String(data);
    }

    /**
     * Set data for a node
     */
    public void setData(String path, String data) throws KeeperException, InterruptedException {
        zooKeeper.setData(path, data.getBytes(), -1);
    }

    /**
     * Get children of a path
     */
    public List<String> getChildren(String path) throws KeeperException, InterruptedException {
        try {
            return zooKeeper.getChildren(path, false);
        } catch (KeeperException.NoNodeException e) {
            return new ArrayList<>();
        }
    }

    /**
     * Create a path recursively
     */
    private void createPath(String path) {
        try {
            if (path.equals("/")) {
                return;
            }

            int lastSlashIndex = path.lastIndexOf('/');
            if (lastSlashIndex > 0) {
                // Create parent path recursively
                String parentPath = path.substring(0, lastSlashIndex);
                createPath(parentPath);
            }

            if (zooKeeper.exists(path, false) == null) {
                zooKeeper.create(path, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                LOGGER.info("Created ZooKeeper path: " + path);
            }
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Failed to create path: " + path, e);
        }
    }

    /**
     * Watch for children changes
     */
    public void watchChildren(String path, ChildrenCallback callback) {
        try {
            List<String> children = zooKeeper.getChildren(path, event -> {
                if (event.getType() == Watcher.Event.EventType.NodeChildrenChanged) {
                    try {
                        List<String> newChildren = zooKeeper.getChildren(path, event2 -> {
                            if (event2.getType() == Watcher.Event.EventType.NodeChildrenChanged) {
                                watchChildren(path, callback);
                            }
                        });
                        callback.onChildrenChanged(newChildren);
                    } catch (Exception e) {
                        LOGGER.log(Level.SEVERE, "Error processing children changed event", e);
                    }
                }
            });
            callback.onChildrenChanged(children);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to watch children for path: " + path, e);
        }
    }

    /**
     * Watch for node changes
     */
    public void watchNode(String path, NodeCallback callback) {
        try {
            zooKeeper.exists(path, event -> {
                if (event.getType() == Watcher.Event.EventType.NodeDeleted) {
                    callback.onNodeChanged();
                } else if (event.getType() == Watcher.Event.EventType.NodeDataChanged) {
                    callback.onNodeChanged();
                } else if (event.getType() == Watcher.Event.EventType.NodeCreated) {
                    callback.onNodeChanged();
                }
            });
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to watch node: " + path, e);
        }
    }

    /**
     * Delete a node
     */
    public void deleteNode(String path) throws KeeperException, InterruptedException {
        if (exists(path)) {
            zooKeeper.delete(path, -1);
            LOGGER.info("Deleted node: " + path);
        }
    }

    /**
     * Process ZooKeeper events
     */
    @Override
    public void process(WatchedEvent event) {
        if (event.getState() == Event.KeeperState.SyncConnected) {
            connectedSignal.countDown();
            LOGGER.info("Connected to ZooKeeper");
        } else if (event.getState() == Event.KeeperState.Disconnected) {
            LOGGER.warning("Disconnected from ZooKeeper");
        } else if (event.getState() == Event.KeeperState.Expired) {
            LOGGER.warning("ZooKeeper session expired, reconnecting...");
            if (reconnect()) {
                LOGGER.info("Reconnected to ZooKeeper after session expiry, restoring state");
                createBasePaths();
                try {
                    newSessionCallback.onNewSession();
                } catch (Exception e) {
                    LOGGER.log(Level.SEVERE, "New session callback failed", e);
                }
            }
        }
    }

    /**
     * Replace the expired session with a new one. Keeps retrying until connected,
     * so a ZooKeeper outage longer than one attempt doesn't leave the broker without a session.
     * Returns false only if the thread was interrupted.
     */
    private boolean reconnect() {
        while (true) {
            try {
                if (zooKeeper != null) {
                    zooKeeper.close();
                }
                connectedSignal = new CountDownLatch(1);
                zooKeeper = new ZooKeeper(getConnectString(), SESSION_TIMEOUT, this);
                if (connectedSignal.await(SESSION_TIMEOUT, TimeUnit.MILLISECONDS)) {
                    return true;
                }
                LOGGER.warning("Reconnect to ZooKeeper timed out, retrying...");
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            } catch (Exception e) {
                LOGGER.log(Level.WARNING, "Failed to reconnect to ZooKeeper, retrying...", e);
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return false;
                }
            }
        }
    }

    /**
     * Callback interface for children changes
     */
    public interface ChildrenCallback {
        void onChildrenChanged(List<String> children);
    }

    /**
     * Callback interface for node changes
     */
    public interface NodeCallback {
        void onNodeChanged();
    }

    /**
     * Callback interface for a new session after expiry
     */
    public interface NewSessionCallback {
        void onNewSession();
    }
}
package com.simplekafka.broker;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class Zookeeper implements Watcher {
    private static final Logger LOGGER = Logger.getLogger(Zookeeper.class.getName());
    private static final int SESSION_TIMEOUT = 30_000;

    private int port;
    private String host;
    private ZooKeeper zooKeeper;
    private CountDownLatch connectedSignal = new CountDownLatch(1);

    public interface ChildrenCallBack {
        void onChildrenChanged(List<String> children);
    }

    public interface NodeCallBack {
        void onNodeChanged();
    }

    public Zookeeper() {
    }

    public Zookeeper(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public ZooKeeper getZooKeeper() {
        return zooKeeper;
    }

    private String getConnectString() {
        return host + ":" + port;
    }

    public synchronized void connect() throws IOException, InterruptedException, KeeperException {
        if (host == null || host.isBlank() || port <= 0) {
            throw new IllegalStateException("ZooKeeper host/port is not configured");
        }

        connectedSignal = new CountDownLatch(1);
        zooKeeper = new ZooKeeper(getConnectString(), SESSION_TIMEOUT, this);
        connectedSignal.await();

        createPath("/brokers");
        createPath("/topics");
        createPath("/controller");
    }

    public synchronized void close() throws InterruptedException {
        if (zooKeeper != null) {
            zooKeeper.close();
            zooKeeper = null;
        }
    }

    public void createPersistentNode(String path, String data) throws KeeperException, InterruptedException {
        Stat stat = zooKeeper.exists(path, false);
        byte[] bytes = data.getBytes(StandardCharsets.UTF_8);
        if (stat == null) {
            zooKeeper.create(path, bytes, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            LOGGER.info("Created persistent node: " + path);
        } else {
            zooKeeper.setData(path, bytes, -1);
            LOGGER.info("Updated persistent node: " + path);
        }
    }

    public void createEphemeralNode(String path, String data) throws KeeperException, InterruptedException {
        Stat stat = zooKeeper.exists(path, false);
        byte[] bytes = data.getBytes(StandardCharsets.UTF_8);
        if (stat == null) {
            zooKeeper.create(path, bytes, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            LOGGER.info("Created ephemeral node: " + path);
        } else {
            zooKeeper.setData(path, bytes, -1);
            LOGGER.info("Updated ephemeral node: " + path);
        }
    }

    public void watchChildren(String path, ChildrenCallBack callback) {
        try {
            List<String> children = zooKeeper.getChildren(path, event -> {
                if (event.getType() == Watcher.Event.EventType.NodeChildrenChanged) {
                    watchChildren(path, callback);
                }
            });
            callback.onChildrenChanged(children);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to watch children for path: " + path, e);
        }
    }

    public void watchNode(String path, NodeCallBack callback) {
        try {
            zooKeeper.exists(path, event -> {
                if (event.getType() == Watcher.Event.EventType.NodeDeleted
                        || event.getType() == Watcher.Event.EventType.NodeDataChanged
                        || event.getType() == Watcher.Event.EventType.NodeCreated) {
                    callback.onNodeChanged();
                }
            });
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to watch node for path: " + path, e);
        }
    }

    @Override
    public void process(WatchedEvent event) {
        if (event.getState() == Event.KeeperState.SyncConnected) {
            connectedSignal.countDown();
            LOGGER.info("Connected to ZooKeeper");
        } else if (event.getState() == Event.KeeperState.Disconnected) {
            LOGGER.warning("Disconnected from ZooKeeper");
        } else if (event.getState() == Event.KeeperState.Expired) {
            LOGGER.warning("ZooKeeper session expired, reconnecting...");
            try {
                if (zooKeeper != null) {
                    zooKeeper.close();
                }
                connectedSignal = new CountDownLatch(1);
                zooKeeper = new ZooKeeper(getConnectString(), SESSION_TIMEOUT, this);
                connectedSignal.await();
                LOGGER.info("Reconnected to ZooKeeper after session expiry");
            } catch (Exception e) {
                LOGGER.log(Level.SEVERE, "Failed to reconnect to ZooKeeper", e);
            }
        }
    }

    private void createPath(String path) throws KeeperException, InterruptedException {
        if (zooKeeper.exists(path, false) == null) {
            zooKeeper.create(path, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            LOGGER.info("Created znode path: " + path);
        }
    }

    public interface ChildrenCallback {
        void onChildrenChanged(List<String> children);
    }

    /**
     * Callback interface for node changes
     */
    public interface NodeCallback {
        void onNodeChanged();
    }
}
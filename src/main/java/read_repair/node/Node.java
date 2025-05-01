package read_repair.node;

import read_repair.model.DataItem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Node {
    private static final Logger logger = LoggerFactory.getLogger(Node.class);
    private final UUID nodeId;
    private final String nodeName;
    private final Map<String, DataItem> dataStore;
    private final ReadWriteLock availabilityLock;
    private final AtomicLong logicalClock;
    private volatile boolean isAvailable;
    private volatile long networkLatency; // Simulated network latency in milliseconds
    private volatile double networkReliability; // Probability of successful network operation (0.0 to 1.0)

    public Node(String nodeName) {
        this.nodeId = UUID.randomUUID();
        this.nodeName = nodeName;
        this.dataStore = new ConcurrentHashMap<>();
        this.availabilityLock = new ReentrantReadWriteLock();
        this.logicalClock = new AtomicLong(0);
        this.isAvailable = true;
        this.networkLatency = 0;
        this.networkReliability = 1.0;
    }

    public UUID getNodeId() {
        return nodeId;
    }

    public String getNodeName() {
        return nodeName;
    }

    public void setNetworkLatency(long latency) {
        this.networkLatency = latency;
    }

    public void setNetworkReliability(double reliability) {
        if (reliability < 0.0 || reliability > 1.0) {
            throw new IllegalArgumentException("Reliability must be between 0.0 and 1.0");
        }
        this.networkReliability = reliability;
    }

    private void simulateNetworkOperation() throws InterruptedException {
        if (Math.random() > networkReliability) {
            throw new RuntimeException("Network operation failed");
        }
        if (networkLatency > 0) {
            Thread.sleep(networkLatency);
        }
    }

    public void write(DataItem dataItem) throws InterruptedException {
        availabilityLock.readLock().lock();
        try {
            if (!isAvailable) {
                throw new IllegalStateException("Node " + nodeName + " is not available");
            }
            simulateNetworkOperation();
            dataStore.put(dataItem.getKey(), dataItem);
            logicalClock.set(Math.max(logicalClock.get(), dataItem.getLogicalClock()));
            logger.debug("Node {} wrote data item: {}", nodeName, dataItem);
        } finally {
            availabilityLock.readLock().unlock();
        }
    }

    public DataItem read(String key) throws InterruptedException {
        availabilityLock.readLock().lock();
        try {
            if (!isAvailable) {
                throw new IllegalStateException("Node " + nodeName + " is not available");
            }
            simulateNetworkOperation();
            DataItem dataItem = dataStore.get(key);
            logger.debug("Node {} read data item: {}", nodeName, dataItem);
            return dataItem;
        } finally {
            availabilityLock.readLock().unlock();
        }
    }

    public void repair(DataItem dataItem) throws InterruptedException {
        availabilityLock.readLock().lock();
        try {
            if (!isAvailable) {
                throw new IllegalStateException("Node " + nodeName + " is not available");
            }
            simulateNetworkOperation();
            DataItem current = dataStore.get(dataItem.getKey());
            if (current == null || dataItem.isNewerThan(current)) {
                dataStore.put(dataItem.getKey(), dataItem);
                logicalClock.set(Math.max(logicalClock.get(), dataItem.getLogicalClock()));
                logger.info("Node {} repaired data item: {}", nodeName, dataItem);
            }
        } finally {
            availabilityLock.readLock().unlock();
        }
    }

    public void setAvailable(boolean available) {
        availabilityLock.writeLock().lock();
        try {
            this.isAvailable = available;
            logger.info("Node {} availability set to: {}", nodeName, available);
        } finally {
            availabilityLock.writeLock().unlock();
        }
    }

    public boolean isAvailable() {
        availabilityLock.readLock().lock();
        try {
            return isAvailable;
        } finally {
            availabilityLock.readLock().unlock();
        }
    }

    public long getLogicalClock() {
        return logicalClock.get();
    }

    public void incrementLogicalClock() {
        logicalClock.incrementAndGet();
    }
} 
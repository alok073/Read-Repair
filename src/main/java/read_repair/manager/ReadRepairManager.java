package read_repair.manager;

import read_repair.model.DataItem;
import read_repair.node.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

public class ReadRepairManager {
    private static final Logger logger = LoggerFactory.getLogger(ReadRepairManager.class);
    private final List<Node> nodes;
    private final int readQuorum;
    private final int writeQuorum;
    private final ExecutorService executorService;
    private final long operationTimeout;
    private final Map<String, ReadWriteLock> keyLocks;

    public ReadRepairManager(List<Node> nodes, int readQuorum, int writeQuorum, long operationTimeout) {
        this.nodes = new ArrayList<>(nodes);
        this.readQuorum = readQuorum;
        this.writeQuorum = writeQuorum;
        this.operationTimeout = operationTimeout;
        this.executorService = Executors.newFixedThreadPool(nodes.size());
        this.keyLocks = new ConcurrentHashMap<>();
        
        if (readQuorum + writeQuorum <= nodes.size()) {
            throw new IllegalArgumentException("Quorum requirements must satisfy R + W > N");
        }
    }

    private ReadWriteLock getKeyLock(String key) {
        return keyLocks.computeIfAbsent(key, k -> new ReentrantReadWriteLock());
    }

    public DataItem readWithRepair(String key) throws InterruptedException, TimeoutException {
        ReadWriteLock keyLock = getKeyLock(key);
        keyLock.readLock().lock();
        try {
            // Read from all available nodes with timeout
            List<Future<DataItem>> futures = nodes.stream()
                    .filter(Node::isAvailable)
                    .map(node -> executorService.submit(() -> node.read(key)))
                    .collect(Collectors.toList());

            // Wait for responses with timeout
            List<DataItem> responses = new ArrayList<>();
            for (Future<DataItem> future : futures) {
                try {
                    DataItem response = future.get(operationTimeout, TimeUnit.MILLISECONDS);
                    if (response != null) {
                        responses.add(response);
                    }
                } catch (ExecutionException | TimeoutException e) {
                    logger.warn("Failed to read from node: {}", e.getMessage());
                }
            }

            // Check quorum
            if (responses.size() < readQuorum) {
                throw new IllegalStateException("Not enough responses for read quorum");
            }

            // Find the most recent version
            DataItem latestVersion = responses.stream()
                    .max((a, b) -> {
                        if (a.getVersion() != b.getVersion()) {
                            return Long.compare(a.getVersion(), b.getVersion());
                        }
                        if (a.getLogicalClock() != b.getLogicalClock()) {
                            return Long.compare(a.getLogicalClock(), b.getLogicalClock());
                        }
                        return Long.compare(a.getTimestamp(), b.getTimestamp());
                    })
                    .orElse(null);

            if (latestVersion == null) {
                return null;
            }

            // Repair nodes that have older versions
            List<Future<?>> repairFutures = nodes.stream()
                    .filter(Node::isAvailable)
                    .map(node -> executorService.submit(() -> {
                        try {
                            node.repair(latestVersion);
                            return null;
                        } catch (Exception e) {
                            logger.error("Failed to repair node: {}", e.getMessage());
                            return null;
                        }
                    }))
                    .collect(Collectors.toList());

            // Wait for repairs to complete
            for (Future<?> future : repairFutures) {
                try {
                    future.get(operationTimeout, TimeUnit.MILLISECONDS);
                } catch (ExecutionException | TimeoutException e) {
                    logger.warn("Repair operation failed: {}", e.getMessage());
                }
            }

            return latestVersion;
        } finally {
            keyLock.readLock().unlock();
        }
    }

    public void write(DataItem dataItem) throws InterruptedException, TimeoutException {
        ReadWriteLock keyLock = getKeyLock(dataItem.getKey());
        keyLock.writeLock().lock();
        try {
            // Write to all available nodes with timeout
            List<Future<?>> futures = nodes.stream()
                    .filter(Node::isAvailable)
                    .map(node -> executorService.submit(() -> {
                        node.write(dataItem);
                        return null;
                    }))
                    .collect(Collectors.toList());

            // Wait for responses with timeout
            int successfulWrites = 0;
            for (Future<?> future : futures) {
                try {
                    future.get(operationTimeout, TimeUnit.MILLISECONDS);
                    successfulWrites++;
                } catch (ExecutionException | TimeoutException e) {
                    logger.warn("Failed to write to node: {}", e.getMessage());
                }
            }

            // Check quorum
            if (successfulWrites < writeQuorum) {
                throw new IllegalStateException("Not enough successful writes for write quorum");
            }
        } finally {
            keyLock.writeLock().unlock();
        }
    }

    public void shutdown() {
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
} 
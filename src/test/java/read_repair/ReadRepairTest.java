package read_repair;

import read_repair.manager.ReadRepairManager;
import read_repair.model.DataItem;
import read_repair.node.Node;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class ReadRepairTest {
    private List<Node> nodes;
    private ReadRepairManager readRepairManager;
    private static final String TEST_KEY = "test-key";
    private static final String TEST_VALUE = "test-value";
    private static final int NUM_NODES = 5;
    private static final int READ_QUORUM = 3;
    private static final int WRITE_QUORUM = 3;
    private static final long OPERATION_TIMEOUT = 5000;

    @BeforeEach
    void setUp() {
        nodes = Arrays.asList(
                new Node("node1"),
                new Node("node2"),
                new Node("node3"),
                new Node("node4"),
                new Node("node5")
        );
        readRepairManager = new ReadRepairManager(nodes, READ_QUORUM, WRITE_QUORUM, OPERATION_TIMEOUT);
    }

    @AfterEach
    void tearDown() {
        readRepairManager.shutdown();
    }

    @Test
    void testBasicReadRepair() throws Exception {
        // Write initial data
        DataItem initialData = new DataItem(TEST_KEY, TEST_VALUE, 1, System.currentTimeMillis(), 
                nodes.get(0).getNodeId(), nodes.get(0).getLogicalClock());
        readRepairManager.write(initialData);

        // Verify all nodes have the data
        for (Node node : nodes) {
            DataItem readData = node.read(TEST_KEY);
            assertNotNull(readData);
            assertEquals(initialData, readData);
        }

        // Update data on one node
        DataItem updatedData = new DataItem(TEST_KEY, "updated-value", 2, System.currentTimeMillis(),
                nodes.get(0).getNodeId(), nodes.get(0).getLogicalClock());
        nodes.get(0).write(updatedData);

        // Read with repair should fix the inconsistency
        DataItem repairedData = readRepairManager.readWithRepair(TEST_KEY);
        assertNotNull(repairedData);
        assertEquals(updatedData, repairedData);

        // Verify all nodes now have the updated data
        for (Node node : nodes) {
            DataItem readData = node.read(TEST_KEY);
            assertNotNull(readData);
            assertEquals(updatedData, readData);
        }
    }

    @Test
    void testConcurrentReadRepair() throws Exception {
        // Write initial data
        DataItem initialData = new DataItem(TEST_KEY, TEST_VALUE, 1, System.currentTimeMillis(),
                nodes.get(0).getNodeId(), nodes.get(0).getLogicalClock());
        readRepairManager.write(initialData);

        // Simulate concurrent updates and reads
        ExecutorService executor = Executors.newFixedThreadPool(10);
        CountDownLatch latch = new CountDownLatch(10);

        for (int i = 0; i < 5; i++) {
            final int nodeIndex = i;
            executor.submit(() -> {
                try {
                    DataItem updatedData = new DataItem(TEST_KEY, "value-" + nodeIndex, 
                            nodeIndex + 2, System.currentTimeMillis(),
                            nodes.get(nodeIndex).getNodeId(), nodes.get(nodeIndex).getLogicalClock());
                    nodes.get(nodeIndex).write(updatedData);
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    latch.countDown();
                }
            });
        }

        for (int i = 0; i < 5; i++) {
            executor.submit(() -> {
                try {
                    readRepairManager.readWithRepair(TEST_KEY);
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await(10, TimeUnit.SECONDS);
        executor.shutdown();
        executor.awaitTermination(5, TimeUnit.SECONDS);

        // Verify consistency after concurrent operations
        DataItem finalData = readRepairManager.readWithRepair(TEST_KEY);
        assertNotNull(finalData);
        
        for (Node node : nodes) {
            DataItem nodeData = node.read(TEST_KEY);
            assertNotNull(nodeData);
            assertEquals(finalData, nodeData);
        }
    }

    @Test
    void testNetworkPartition() throws Exception {
        // Write initial data
        DataItem initialData = new DataItem(TEST_KEY, TEST_VALUE, 1, System.currentTimeMillis(),
                nodes.get(0).getNodeId(), nodes.get(0).getLogicalClock());
        readRepairManager.write(initialData);

        // Simulate network partition by making some nodes unavailable
        nodes.get(0).setAvailable(false);
        nodes.get(1).setAvailable(false);

        // Update data on available nodes
        DataItem updatedData = new DataItem(TEST_KEY, "updated-value", 2, System.currentTimeMillis(),
                nodes.get(2).getNodeId(), nodes.get(2).getLogicalClock());
        nodes.get(2).write(updatedData);

        // Read should still work with remaining available nodes
        DataItem readData = readRepairManager.readWithRepair(TEST_KEY);
        assertNotNull(readData);
        assertEquals(updatedData, readData);

        // Heal partition
        nodes.get(0).setAvailable(true);
        nodes.get(1).setAvailable(true);

        // Read again to verify repair
        DataItem finalData = readRepairManager.readWithRepair(TEST_KEY);
        assertNotNull(finalData);
        assertEquals(updatedData, finalData);

        // Verify all nodes have the same data
        for (Node node : nodes) {
            DataItem nodeData = node.read(TEST_KEY);
            assertNotNull(nodeData);
            assertEquals(updatedData, nodeData);
        }
    }

    @Test
    void testNetworkLatency() throws Exception {
        // Set different network latencies for nodes
        nodes.get(0).setNetworkLatency(100);
        nodes.get(1).setNetworkLatency(200);
        nodes.get(2).setNetworkLatency(300);
        nodes.get(3).setNetworkLatency(400);
        nodes.get(4).setNetworkLatency(500);

        // Write initial data
        DataItem initialData = new DataItem(TEST_KEY, TEST_VALUE, 1, System.currentTimeMillis(),
                nodes.get(0).getNodeId(), nodes.get(0).getLogicalClock());
        readRepairManager.write(initialData);

        // Read should still work despite different latencies
        DataItem readData = readRepairManager.readWithRepair(TEST_KEY);
        assertNotNull(readData);
        assertEquals(initialData, readData);
    }

    @Test
    void testNetworkReliability() throws Exception {
        // Set different network reliability for nodes
        nodes.get(0).setNetworkReliability(0.9);
        nodes.get(1).setNetworkReliability(0.8);
        nodes.get(2).setNetworkReliability(0.7);
        nodes.get(3).setNetworkReliability(0.6);
        nodes.get(4).setNetworkReliability(0.5);

        // Write initial data
        DataItem initialData = new DataItem(TEST_KEY, TEST_VALUE, 1, System.currentTimeMillis(),
                nodes.get(0).getNodeId(), nodes.get(0).getLogicalClock());
        readRepairManager.write(initialData);

        // Read should still work despite network unreliability
        DataItem readData = readRepairManager.readWithRepair(TEST_KEY);
        assertNotNull(readData);
        assertEquals(initialData, readData);
    }

    @Test
    void testInsufficientQuorum() throws Exception {
        // Write initial data
        DataItem initialData = new DataItem(TEST_KEY, TEST_VALUE, 1, System.currentTimeMillis(),
                nodes.get(0).getNodeId(), nodes.get(0).getLogicalClock());
        readRepairManager.write(initialData);

        // Make too many nodes unavailable
        nodes.get(0).setAvailable(false);
        nodes.get(1).setAvailable(false);
        nodes.get(2).setAvailable(false);

        // Attempting to read should throw an exception due to insufficient quorum
        assertThrows(IllegalStateException.class, () -> readRepairManager.readWithRepair(TEST_KEY));
    }
} 
# Read Repair Implementation

## What is Read Repair?

Read Repair is a consistency maintenance technique used in distributed systems. It helps maintain data consistency across multiple replicas by automatically repairing inconsistent data during read operations.

### Key Concepts:
- **Replicas**: Multiple copies of the same data stored on different nodes
- **Consistency**: Ensuring all replicas have the same data
- **Quorum**: Minimum number of nodes that must participate in read/write operations
- **Versioning**: Tracking data changes using version numbers and timestamps
- **Logical Clock**: Used for causal ordering of operations

## Implementation Overview

This project implements a distributed read repair system with the following features:

1. **Thread-safe Operations**
2. **Quorum-based Reads and Writes**
3. **Network Partition Handling**
4. **Concurrent Operation Support**
5. **Network Latency and Reliability Simulation**

### Core Components

1. **DataItem**
   - Represents a piece of data with version information
   - Contains key, value, version, timestamp, node ID, and logical clock
   - Implements version comparison logic

2. **Node**
   - Represents a replica in the distributed system
   - Manages its own data store
   - Simulates network conditions (latency, reliability)
   - Handles read, write, and repair operations

3. **ReadRepairManager**
   - Orchestrates read repair operations
   - Manages quorum requirements
   - Handles concurrent operations
   - Coordinates repairs across nodes

## Pseudo Code

### Read Repair Algorithm
```
function readWithRepair(key):
    responses = []
    
    # Read from all available nodes
    for node in nodes:
        if node.isAvailable():
            response = node.read(key)
            if response != null:
                responses.add(response)
    
    # Check quorum
    if responses.size() < readQuorum:
        throw InsufficientQuorumException
    
    # Find latest version
    latestVersion = findLatestVersion(responses)
    
    # Repair nodes with older versions
    for node in nodes:
        if node.isAvailable():
            current = node.read(key)
            if current == null or current.version < latestVersion.version:
                node.repair(latestVersion)
    
    return latestVersion

function write(data):
    successfulWrites = 0
    
    # Write to all available nodes
    for node in nodes:
        if node.isAvailable():
            node.write(data)
            successfulWrites++
    
    # Check quorum
    if successfulWrites < writeQuorum:
        throw InsufficientQuorumException
```

## Understanding the Implementation through Tests

The best way to understand this implementation is through the test cases in `ReadRepairTest.java`. Each test demonstrates a different aspect of the system:

1. **Basic Read Repair** (`testBasicReadRepair`)
   - Demonstrates the fundamental read repair mechanism
   - Shows how inconsistencies are detected and repaired
   - Verifies data consistency after repair

2. **Concurrent Operations** (`testConcurrentReadRepair`)
   - Tests the system under concurrent read/write operations
   - Verifies thread safety and consistency
   - Demonstrates handling of race conditions

3. **Network Partition** (`testNetworkPartition`)
   - Simulates network partitions
   - Shows how the system handles node unavailability
   - Demonstrates repair after partition healing

4. **Network Conditions** (`testNetworkLatency`, `testNetworkReliability`)
   - Tests system behavior under different network conditions
   - Simulates network latency and reliability issues
   - Verifies operation timeouts and error handling

5. **Quorum Validation** (`testInsufficientQuorum`)
   - Tests quorum requirements
   - Demonstrates system behavior when quorum cannot be met
   - Verifies proper error handling

## Running the Tests

```bash
mvn test
```

## Future Enhancements

1. **Hinted Handoff**
   - Store writes for temporarily unavailable nodes
   - Deliver writes when nodes become available

2. **Anti-Entropy Repair**
   - Background repair process
   - Merkle trees for efficient comparison

3. **Monitoring and Metrics**
   - Performance metrics collection
   - Health monitoring
   - Alerting system

4. **More Sophisticated Quorum Strategies**
   - Dynamic quorum adjustment
   - Region-aware quorums
   - Weighted quorums 
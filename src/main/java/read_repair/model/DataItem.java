package read_repair.model;

import java.util.Objects;
import java.util.UUID;

public class DataItem {
    private final String key;
    private final String value;
    private final long version;
    private final long timestamp;
    private final UUID nodeId;  // ID of the node that last updated this item
    private final long logicalClock;  // For causal ordering

    public DataItem(String key, String value, long version, long timestamp, UUID nodeId, long logicalClock) {
        this.key = Objects.requireNonNull(key, "Key cannot be null");
        this.value = Objects.requireNonNull(value, "Value cannot be null");
        this.version = version;
        this.timestamp = timestamp;
        this.nodeId = Objects.requireNonNull(nodeId, "Node ID cannot be null");
        this.logicalClock = logicalClock;
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }

    public long getVersion() {
        return version;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public UUID getNodeId() {
        return nodeId;
    }

    public long getLogicalClock() {
        return logicalClock;
    }

    public boolean isNewerThan(DataItem other) {
        if (other == null) return true;
        if (this.version != other.version) {
            return this.version > other.version;
        }
        if (this.logicalClock != other.logicalClock) {
            return this.logicalClock > other.logicalClock;
        }
        return this.timestamp > other.timestamp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DataItem dataItem = (DataItem) o;
        return version == dataItem.version &&
                timestamp == dataItem.timestamp &&
                logicalClock == dataItem.logicalClock &&
                Objects.equals(key, dataItem.key) &&
                Objects.equals(value, dataItem.value) &&
                Objects.equals(nodeId, dataItem.nodeId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, value, version, timestamp, nodeId, logicalClock);
    }

    @Override
    public String toString() {
        return "DataItem{" +
                "key='" + key + '\'' +
                ", value='" + value + '\'' +
                ", version=" + version +
                ", timestamp=" + timestamp +
                ", nodeId=" + nodeId +
                ", logicalClock=" + logicalClock +
                '}';
    }
} 
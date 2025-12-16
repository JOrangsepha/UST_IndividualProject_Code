package com.example.tpch;

import java.io.Serializable;

public class DeleteEvent implements Serializable {
    private String tableName;
    private String keyValue;
    private String reason;
    private long timestamp;

    public DeleteEvent() {}

    public DeleteEvent(String tableName, String keyValue, String reason) {
        this.tableName = tableName;
        this.keyValue = keyValue;
        this.reason = reason;
        this.timestamp = System.currentTimeMillis();
    }

    // Getter and Setter methods
    public String getTableName() { return tableName; }
    public void setTableName(String tableName) { this.tableName = tableName; }

    public String getKeyValue() { return keyValue; }
    public void setKeyValue(String keyValue) { this.keyValue = keyValue; }

    public String getReason() { return reason; }
    public void setReason(String reason) { this.reason = reason; }

    public long getTimestamp() { return timestamp; }
    public void setTimestamp(long timestamp) { this.timestamp = timestamp; }

    @Override
    public String toString() {
        return String.format("DeleteEvent{table='%s', key='%s', reason='%s'}",
                tableName, keyValue, reason);
    }
}
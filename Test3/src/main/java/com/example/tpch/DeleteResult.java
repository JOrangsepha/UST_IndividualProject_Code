package com.example.tpch;

import java.io.Serializable;
import java.util.Date;

/**
 * 删除结果类 - 表示删除操作的结果
 */
public class DeleteResult implements Serializable {
    private String tableName;
    private String keyValue;
    private String recordData;
    private String cascadeFrom;
    private long timestamp;

    public DeleteResult() {}

    public DeleteResult(String tableName, String keyValue, String recordData, String cascadeFrom) {
        this.tableName = tableName;
        this.keyValue = keyValue;
        this.recordData = recordData;
        this.cascadeFrom = cascadeFrom;
        this.timestamp = System.currentTimeMillis();
    }

    // Getter and Setter methods
    public String getTableName() { return tableName; }
    public void setTableName(String tableName) { this.tableName = tableName; }

    public String getKeyValue() { return keyValue; }
    public void setKeyValue(String keyValue) { this.keyValue = keyValue; }

    public String getRecordData() { return recordData; }
    public void setRecordData(String recordData) { this.recordData = recordData; }

    public String getCascadeFrom() { return cascadeFrom; }
    public void setCascadeFrom(String cascadeFrom) { this.cascadeFrom = cascadeFrom; }

    public long getTimestamp() { return timestamp; }
    public void setTimestamp(long timestamp) { this.timestamp = timestamp; }

    @Override
    public String toString() {
        return String.format("DELETE: table=%-10s key=%-8s original=%-15s data=%-30s time=%tT",
                tableName, keyValue, cascadeFrom, recordData, new Date(timestamp));
    }
}
package com.example.tpch;

import java.io.Serializable;

public class PerformanceMetric implements Serializable {
    private long timestamp;
    private int totalDeletes;
    private int userInitiatedDeletes;
    private boolean isFinal = false;

    public PerformanceMetric() {}

    public PerformanceMetric(int totalDeletes, int userInitiatedDeletes) {
        this.timestamp = System.currentTimeMillis();
        this.totalDeletes = totalDeletes;
        this.userInitiatedDeletes = userInitiatedDeletes;
    }

    // Getter and Setter methods
    public long getTimestamp() { return timestamp; }
    public void setTimestamp(long timestamp) { this.timestamp = timestamp; }

    public int getTotalDeletes() { return totalDeletes; }
    public void setTotalDeletes(int totalDeletes) { this.totalDeletes = totalDeletes; }

    public int getUserInitiatedDeletes() { return userInitiatedDeletes; }
    public void setUserInitiatedDeletes(int userInitiatedDeletes) { this.userInitiatedDeletes = userInitiatedDeletes; }

    public boolean isFinal() { return isFinal; }
    public void setFinal(boolean isFinal) { this.isFinal = isFinal; }

    @Override
    public String toString() {
        return String.format("PerformanceMetric{total=%,d, userInitiated=%,d, final=%s}",
                totalDeletes, userInitiatedDeletes, isFinal);
    }
}
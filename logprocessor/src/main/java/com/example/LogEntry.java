package com.example;

import java.time.LocalDateTime;

public class LogEntry {
    private LocalDateTime timestamp;
    private String logLevel;
    private String userId;
    private String action;
    private String details;

    public LogEntry(LocalDateTime timestamp, String logLevel, String userId, String action, String details) {
        this.timestamp = timestamp;
        this.logLevel = logLevel;
        this.userId = userId;
        this.action = action;
        this.details = details;
    }

    public LocalDateTime getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(LocalDateTime timestamp) {
        this.timestamp = timestamp;
    }

    public String getLogLevel() {
        return logLevel;
    }

    public void setLogLevel(String logLevel) {
        this.logLevel = logLevel;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getAction() {
        return action;
    }

    public void setAction(String action) {
        this.action = action;
    }

    public String getDetails() {
        return details;
    }

    public void setDetails(String details) {
        this.details = details;
    }
}
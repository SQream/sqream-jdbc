package com.sqream.jdbc.logging;

public enum LoggerLevel {
    OFF("OFF"),
    DEBUG("DEBUG"),
    TRACE("TRACE");

    private String value;

    LoggerLevel(String level) {
        this.value = level;
    }

    public String getValue() {
        return this.value;
    }
}

package com.sqream.jdbc.enums;

public enum DriverProperties {
    CLUSTER("cluster"),
    SSL("ssl"),
    SERVICE("service"),
    SCHEMA("schema"),
    PROVIDER("provider"),
    HOST("host"),
    PORT("port"),
    DB_NAME("dbName"),
    USER("user"),
    PASSWORD("password"),
    LOGGER_LEVEL("loggerLevel"),
    LOG_FILE_PATH("logFile"),
    INSERT_BUFFER("insertBuffer");



    private String value;

    DriverProperties(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }


    @Override
    public String toString() {
        return this.getValue();
    }
}

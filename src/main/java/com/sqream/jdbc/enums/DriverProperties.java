package com.sqream.jdbc.enums;

public enum DriverProperties {
    CLUSTER("cluster"),
    SSL("ssl"),
    SERVICE("service"),
    SCHEMA("schema"),
    PROVIDER("provider"),
    HOST("host"),
    PORT("port"),
    DB_NAME("dbname"),
    USER("user"),
    PASSWORD("password"),
    LOGGER_LEVEL("loggerlevel"),
    LOG_FILE_PATH("logfile"),
    INSERT_BUFFER("insertbuffer");



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

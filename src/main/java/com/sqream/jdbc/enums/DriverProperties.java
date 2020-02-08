package com.sqream.jdbc.enums;

public enum DriverProperties {
    CLUSTER("cluster"),
    SSL("ssl"),
    SERVICE("service"),
    SCHEMA("schema"),
    SKIP_PICKER("SkipPicker");


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

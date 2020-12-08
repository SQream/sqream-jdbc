package com.sqream.jdbc.connector.enums;

public enum StatementType {
    NON_QUERY("NON_QUERY"),
    NETWORK_INSERT("NETWORK_INSERT"),
    QUERY("QUERY");

    private String value;

    StatementType(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}

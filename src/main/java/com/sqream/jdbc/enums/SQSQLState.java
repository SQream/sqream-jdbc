package com.sqream.jdbc.enums;

public enum SQSQLState {
    INVALID_AUTHORIZATION_SPECIFICATION("28000"),
    INVALID_CATALOG_NAME("3D000");

    private final String state;

    SQSQLState(String state) {
        this.state = state;
    }

    public String getState() {
        return state;
    }
}

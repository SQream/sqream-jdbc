package com.sqream.jdbc.connector.enums;

public enum StatementType {
    DML("DML"),
    INSERT("INSERT"),
    SELECT("SELECT");

    private String value;

    StatementType(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}
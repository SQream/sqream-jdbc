package com.sqream.jdbc.connector;

public enum ColumnTypes {

    FT_SHORT("ftShort"),
    FT_UBYTE("ftUByte"),
    FT_INT("ftInt"),
    FT_LONG("ftLong"),
    FT_FLOAT("ftFloat"),
    FT_DOUBLE("ftDouble"),
    FT_BOOL("ftBool"),
    FT_DATE("ftDate"),
    FT_DATE_TIME("ftDateTime");

    private String value;

    ColumnTypes(String value) {
        this.value = value;
    }

    public String getValue() {
        return this.value;
    }
}

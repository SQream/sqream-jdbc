package com.sqream.jdbc.connector;

public class ColumnMetadataDto {

    private boolean truVarchar;
    private String name;
    private boolean nullable;
    private String valueType;
    private int valueSize;
    private int scale;

    public ColumnMetadataDto(boolean truVarchar, String name, boolean nullable, String valueType, int valueSize, int scale) {
        this.truVarchar = truVarchar;
        this.name = name;
        this.nullable = nullable;
        this.valueType = valueType;
        this.valueSize = valueSize;
        this.scale = scale;
    }

    public boolean isTruVarchar() {
        return truVarchar;
    }

    public String getName() {
        return name;
    }

    public boolean isNullable() {
        return nullable;
    }

    public String getValueType() {
        return valueType;
    }

    public int getValueSize() {
        return valueSize;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getScale() {
        return scale;
    }
}

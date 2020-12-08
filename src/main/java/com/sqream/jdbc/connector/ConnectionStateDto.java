package com.sqream.jdbc.connector;

public class ConnectionStateDto {

    private int connectionId;
    private String varcharEncoding;

    public ConnectionStateDto(int connectionId, String varcharEncoding) {
        this.connectionId = connectionId;
        this.varcharEncoding = varcharEncoding;
    }

    public int getConnectionId() {
        return connectionId;
    }

    public String getVarcharEncoding() {
        return varcharEncoding;
    }
}

package com.sqream.jdbc.connector;

public class ConnectionStateDto {

    private int connection_id;
    private String varcharEncoding;

    public ConnectionStateDto(int connection_id, String varcharEncoding) {
        this.connection_id = connection_id;
        this.varcharEncoding = varcharEncoding;
    }

    public int getConnectionId() {
        return connection_id;
    }

    public String getVarcharEncoding() {
        return varcharEncoding;
    }
}

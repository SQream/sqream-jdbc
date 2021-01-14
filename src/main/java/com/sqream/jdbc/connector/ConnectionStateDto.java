package com.sqream.jdbc.connector;

public class ConnectionStateDto {

    private final int connectionId;
    private final String varcharEncoding;
    private final String serverVersion;

    public ConnectionStateDto(int connectionId, String varcharEncoding, String serverVersion) {
        this.connectionId = connectionId;
        this.varcharEncoding = varcharEncoding;
        this.serverVersion = serverVersion;
    }

    public int getConnectionId() {
        return connectionId;
    }

    public String getVarcharEncoding() {
        return varcharEncoding;
    }

    public String getServerVersion() {
        return serverVersion;
    }
}

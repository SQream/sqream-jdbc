package com.sqream.jdbc.connector;

public class StatementStateDto {

    private int listener_id;
    private int port;
    private int port_ssl;
    private boolean reconnect;
    private String ip;

    public StatementStateDto(int listener_id, int port, int port_ssl, boolean reconnect, String ip) {
        this.listener_id = listener_id;
        this.port = port;
        this.port_ssl = port_ssl;
        this.reconnect = reconnect;
        this.ip = ip;
    }

    public int getListenerId() {
        return listener_id;
    }

    public int getPort() {
        return port;
    }

    public int getPortSsl() {
        return port_ssl;
    }

    public boolean isReconnect() {
        return reconnect;
    }

    public String getIp() {
        return ip;
    }
}

package com.sqream.jdbc;

public class ConnectionParams {

    private Boolean cluster;
    private String lbip;
    private int lbport;
    private String ip;
    private int port;
    private String user;
    private String password;
    private String schema;
    private String dbName;
    private Boolean useSsl;
    private String service;

    public Boolean getCluster() {
        return cluster;
    }

    public void setCluster(Boolean cluster) {
        this.cluster = cluster;
    }

    public String getLbip() {
        return lbip;
    }

    public void setLbip(String lbip) {
        this.lbip = lbip;
    }

    public int getLbport() {
        return lbport;
    }

    public void setLbport(int lbport) {
        this.lbport = lbport;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public Boolean getUseSsl() {
        return useSsl;
    }

    public void setUseSsl(Boolean useSsl) {
        this.useSsl = useSsl;
    }

    public String getService() {
        return service;
    }

    public void setService(String service) {
        this.service = service;
    }
}

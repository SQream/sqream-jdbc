package com.sqream.jdbc;

import com.sqream.jdbc.connector.ConnException;

public class ConnectionParams {

    private Boolean cluster;
    private String ip;
    private int port;
    private String user;
    private String password;
    private String schema;
    private String dbName;
    private Boolean useSsl;
    private String service;
    private Integer fetchSize;
    private Integer insertBuffer;

    private ConnectionParams() { }

    public static ConnectionParamsBuilder builder() {
        return new ConnectionParamsBuilder();
    }

    public Boolean getCluster() {
        return cluster;
    }

    public String getIp() {
        return ip;
    }

    public int getPort() {
        return port;
    }

    public String getUser() {
        return user;
    }

    public String getPassword() {
        return password;
    }

    public String getSchema() {
        return schema;
    }

    public String getDbName() {
        return dbName;
    }

    public Boolean getUseSsl() {
        return useSsl;
    }

    public String getService() {
        return service;
    }

    public Integer getFetchSize() {
        return fetchSize;
    }

    public Integer getInsertBuffer() {
        return insertBuffer;
    }

    @Override
    public String toString() {
        return "ConnectionParams{" +
                "cluster=" + cluster +
                ", ip='" + ip + '\'' +
                ", port=" + port +
                ", user='" + user + '\'' +
                ", password='" + password + '\'' +
                ", schema='" + schema + '\'' +
                ", dbName='" + dbName + '\'' +
                ", useSsl=" + useSsl +
                ", service='" + service + '\'' +
                '}';
    }

    public static class ConnectionParamsBuilder {
        private boolean cluster;
        private String ip;
        private int port;
        private String dbName;
        private String service;
        private String schema;
        private String user;
        private String password;
        private boolean ssl;
        private Integer fetchSize = null;
        private Integer insertBuffer = null;

        public ConnectionParamsBuilder cluster(String cluster) {
            this.cluster = "true".equalsIgnoreCase(cluster);
            return this;
        }

        public ConnectionParamsBuilder ipAddress(String ipAddress) throws ConnException {
            if ((this.ip = ipAddress) == null) {
                throw new ConnException("missing host error");
            }
            return this;
        }

        public ConnectionParamsBuilder port(String port) throws ConnException {
            if (port == null) {
                throw new ConnException("missing port error");
            }
            this.port = Integer.parseInt(port);
            return this;
        }

        public ConnectionParamsBuilder dbName(String dbName) throws ConnException {
            if ((this.dbName = dbName) == null) {
                throw new ConnException("missing database name error");
            }
            return this;
        }

        public ConnectionParamsBuilder service(String service) throws ConnException {
            if((this.service = service) == null) {
                throw new ConnException("missing service name error");
            }
            return this;
        }

        public ConnectionParamsBuilder schema(String schema) throws ConnException {
            if((this.schema = schema) == null) {
                throw new ConnException("missing schema name error");
            }
            return this;
        }

        public ConnectionParamsBuilder user(String user) throws ConnException {
            if ((this.user = user) == null) {
                throw new ConnException("missing user");
            }
            return this;
        }

        public ConnectionParamsBuilder password(String password) throws ConnException {
            if ((this.password = password) == null) {
                throw new ConnException("missing password");
            }
            return this;
        }

        public ConnectionParamsBuilder useSsl(String ssl) {
            this.ssl = "true".equalsIgnoreCase(ssl);
            return this;
        }

        public ConnectionParamsBuilder fetchSize(String fetchSize) {
            if (fetchSize != null) {
                this.fetchSize = Integer.parseInt(fetchSize);
            }
            return this;
        }

        public ConnectionParamsBuilder insertBuffer(String insertBuffer) {
            if (insertBuffer != null) {
                this.insertBuffer = Integer.parseInt(insertBuffer);
            }
            return this;
        }

        public ConnectionParamsBuilder from(ConnectionParams params) throws ConnException {
            if (params == null) {
                throw new ConnException("Previous connection params can not be null");
            }
            this.cluster = params.cluster;
            this.ip = params.ip;
            this.port = params.port;
            this.dbName = params.dbName;
            this.service = params.service;
            this.schema = params.schema;
            this.user = params.user;
            this.password = params.password;
            this.ssl = params.useSsl;
            this.fetchSize = params.fetchSize;
            this.insertBuffer = params.insertBuffer;
            return this;
        }

        public ConnectionParams build() {
            ConnectionParams result = new ConnectionParams();
            result.cluster = this.cluster;
            result.ip = this.ip;
            result.port = this.port;
            result.dbName = this.dbName;
            result.service = this.service;
            result.schema = this.schema;
            result.user = this.user;
            result.password = this.password;
            result.useSsl = this.ssl;
            result.fetchSize = this.fetchSize;
            return result;
        }
    }
}

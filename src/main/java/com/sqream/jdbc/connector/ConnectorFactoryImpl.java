package com.sqream.jdbc.connector;

import javax.script.ScriptException;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;

public class ConnectorFactoryImpl extends ConnectorFactory {
    @Override
    public Connector initConnector(String ip, int port, boolean cluster, boolean ssl) throws KeyManagementException, ScriptException,
            NoSuchAlgorithmException, IOException {
        return new ConnectorImpl(ip, port, cluster, ssl);
    }
}

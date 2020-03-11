package com.sqream.jdbc.connector;

import javax.script.ScriptException;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;

public class ConnectorFactory {

    public static Connector initConnector(String ip, int port, boolean cluster, boolean ssl) throws ConnException {
        return new ConnectorImpl(ip, port, cluster, ssl);
    }
}

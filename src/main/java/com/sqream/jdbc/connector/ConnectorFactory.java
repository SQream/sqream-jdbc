package com.sqream.jdbc.connector;

import javax.script.ScriptException;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;

public abstract class ConnectorFactory {

    public static ConnectorFactory getFactory() {
        return new ConnectorFactoryImpl();
    }

    abstract public Connector initConnector(String ip, int port, boolean cluster, boolean ssl) throws
            KeyManagementException, ScriptException, NoSuchAlgorithmException, ConnectorImpl.ConnException, IOException;
}

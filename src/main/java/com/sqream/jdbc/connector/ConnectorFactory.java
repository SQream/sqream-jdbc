package com.sqream.jdbc.connector;

import com.sqream.jdbc.ConnectionParams;

import javax.script.ScriptException;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;

public class ConnectorFactory {

    public Connector createConnector(ConnectionParams connParams) throws ConnException {
        return new ConnectorImpl(connParams);
    }
}

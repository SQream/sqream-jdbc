package com.sqream.jdbc.connector.serverAPI;

import com.sqream.jdbc.ConnectionParams;
import com.sqream.jdbc.connector.ConnException;
import com.sqream.jdbc.connector.ConnectionStateDto;
import com.sqream.jdbc.connector.JsonParser;
import com.sqream.jdbc.connector.messenger.Messenger;
import com.sqream.jdbc.connector.messenger.MessengerImpl;
import com.sqream.jdbc.connector.socket.SQSocket;
import com.sqream.jdbc.connector.socket.SQSocketConnector;

public class SqreamConnectionFactory {
    private static final int DEFAULT_CHUNK_SIZE = 1_000_000;

    public static SqreamConnection openConnection(ConnectionParams connParams) throws ConnException {
        SQSocketConnector socket = new SQSocketConnector(new SQSocket());
        socket.connect(connParams.getIp(), connParams.getPort(), connParams.getUseSsl(), connParams.getCluster());
        Messenger messenger = MessengerImpl.getInstance(socket, new JsonParser());
        ConnectionStateDto connectionState = messenger.connect(
                connParams.getDbName(), connParams.getUser(), connParams.getPassword(), connParams.getService());
        SqreamConnectionContext context =
                new SqreamConnectionContext(connectionState, connParams, DEFAULT_CHUNK_SIZE, messenger);
        return new SqreamConnection(context);
    }
}

package com.sqream.jdbc.connector.socket;

import com.sqream.jdbc.connector.ConnException;

import java.nio.ByteBuffer;
import java.util.logging.Level;
import java.util.logging.Logger;

public class SQLoggableSocket extends SQSocket {
    private static final Logger LOGGER = Logger.getLogger(SQLoggableSocket.class.getName());

    SQLoggableSocket(String ip, int port, boolean useSsl) throws ConnException {
        super(ip, port, useSsl);
    }

    @Override
    public int read(ByteBuffer result) throws ConnException {
        LOGGER.log(Level.FINEST, String.format("Start to read. Buffer=[%s]", result));
        int bytesRead = super.read(result);
        LOGGER.log(Level.FINE, String.format("Has read [%s] bytes.", bytesRead));
        return bytesRead;
    }

    void write(ByteBuffer data) throws ConnException {
        LOGGER.log(Level.FINEST, String.format("Start to write [%s] bytes.", data.capacity()));
        super.write(data);
        LOGGER.log(Level.FINE, "Data has been written.");
    }
}

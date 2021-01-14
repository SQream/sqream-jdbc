package com.sqream.jdbc.connector.serverAPI;

import com.sqream.jdbc.connector.ConnException;
import com.sqream.jdbc.connector.heartbeat.HeartBeatService;
import com.sqream.jdbc.connector.heartbeat.HeartBeatServiceFactory;
import com.sqream.jdbc.connector.serverAPI.Statement.SqreamCreatedStatement;
import com.sqream.jdbc.connector.serverAPI.Statement.SqreamCreatedStatementImpl;

import java.text.MessageFormat;

import static com.sqream.jdbc.connector.serverAPI.enums.StatementPhase.*;

public class SqreamConnection implements AutoCloseable {

    private final SqreamConnectionContext context;
    private boolean isOpen;

    public SqreamConnection(SqreamConnectionContext context) {
        this.context = context;
        isOpen = true;
    }

    @Override
    public void close() throws Exception {
        if (isOpen) {
            isOpen = false;
            context.getMessenger().closeConnection();
        }
    }

    public SqreamCreatedStatement createStatement() {
        if (context.getStatementId() != null && context.getStatementPhase() != CLOSED) {
            throw new RuntimeException(MessageFormat.format(
                    "Did not close statement with id [{0}] before prepare new one.", context.getStatementId()));
        }
        try {
            context.setStatementId(context.getMessenger().openStatement());
            HeartBeatService heartBeatService = HeartBeatServiceFactory.getService(
                    context.getConnState().getServerVersion(),
                    context.getMessenger());
            context.setPingService(heartBeatService);
            return new SqreamCreatedStatementImpl(context);
        } catch (ConnException e) { //TODO needs refactor to throw correct exception Alex K 11.11.2020
            throw new RuntimeException(e);
        }
    }

    public int getId() {
        return context.getConnState().getConnectionId();
    }

    public boolean isOpen() {
        return isOpen;
    }
}

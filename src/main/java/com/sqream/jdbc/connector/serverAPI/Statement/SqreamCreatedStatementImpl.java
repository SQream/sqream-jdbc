package com.sqream.jdbc.connector.serverAPI.Statement;

import com.sqream.jdbc.ConnectionParams;
import com.sqream.jdbc.connector.ConnException;
import com.sqream.jdbc.connector.StatementStateDto;
import com.sqream.jdbc.connector.serverAPI.SqreamConnectionContext;
import com.sqream.jdbc.connector.serverAPI.enums.StatementPhase;

public class SqreamCreatedStatementImpl extends BasesProtocolPhase implements SqreamCreatedStatement {
    /**
     * Used to check if prepared statement has been called already
     */
    private boolean prepared;

    public SqreamCreatedStatementImpl(SqreamConnectionContext context) {
        super(context);
    }

    @Override
    protected StatementPhase getStatementPhase() {
        return StatementPhase.CREATED;
    }

    @Override
    public SqreamPreparedStatement prepare(String query) throws ConnException {
        if (prepared) {
            throw new RuntimeException("Statement has been prepared already");
        }
        try {
            context.setQuery(query);
            StatementStateDto statementState = null;
            statementState = context.getMessenger().prepareStatement(query, context.getChunkSize());
            ConnectionParams connParams = context.getConnParams();
            int port = connParams.getUseSsl() ? statementState.getPortSsl() : statementState.getPort();
            // Reconnect and reestablish statement if redirected by load balancer
            if (statementState.isReconnect()) {
                context.getSocket().reconnect(statementState.getIp(), port, connParams.getUseSsl());
                // Sending reconnect, reconstruct commands
                context.getMessenger().reconnect(connParams.getDbName(), connParams.getUser(),
                        connParams.getPassword(), connParams.getService(),
                        context.getConnState().getConnectionId(), statementState.getListenerId());
                context.getMessenger().isStatementReconstructed(context.getStatementId());
            }
            prepared = true;
            return new SqreamPreparedStatementImpl(context);
        } catch (ConnException e) {
            close();
            throw e;
        }
    }
}

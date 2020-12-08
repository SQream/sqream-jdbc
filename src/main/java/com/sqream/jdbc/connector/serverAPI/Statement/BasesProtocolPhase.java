package com.sqream.jdbc.connector.serverAPI.Statement;

import com.sqream.jdbc.connector.ConnException;
import com.sqream.jdbc.connector.serverAPI.SqreamConnectionContext;
import com.sqream.jdbc.connector.serverAPI.enums.StatementPhase;

import static com.sqream.jdbc.connector.serverAPI.enums.StatementPhase.CLOSED;

public abstract class BasesProtocolPhase implements SqreamStatement, CloseableSqreamStatement {
    protected SqreamConnectionContext context;

    public BasesProtocolPhase(SqreamConnectionContext context) {
        this.context = context;
        this.context.setStatementPhase(getStatementPhase());
    }

    protected abstract StatementPhase getStatementPhase();

    @Override
    public void close() {
        switch (context.getStatementPhase()) {
            case CLOSED:
                break;
            case CREATED:
                closeStatementQuiet();
                break;
            default:
                closeStatement();
        }
    }

    @Override
    public boolean isOpen() {
        return context != null && !CLOSED.equals(context.getStatementPhase());
    }

    @Override
    public int getId() {
        return context.getStatementId();
    }

    /**
     * Used when close statement on the server side is not necessary or prohibited.
     * Just switch statement to {@link StatementPhase#CLOSED}.
     */
    private void closeStatementQuiet() {
        context.setStatementPhase(CLOSED);
    }

    /**
     * Close statement on the server side and switch statement to {@link StatementPhase#CLOSED}.
     */
    private void closeStatement() {
        try {
            context.getMessenger().closeStatement();
        } catch (ConnException e) { //TODO needs refactor to throw correct exception Alex K 11.11.2020
            throw new RuntimeException(e);
        } finally {
            context.setStatementPhase(CLOSED);
        }
    }
}

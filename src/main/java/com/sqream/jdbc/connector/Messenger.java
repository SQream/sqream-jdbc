package com.sqream.jdbc.connector;

import java.io.IOException;
import java.text.MessageFormat;

class Messenger {
    private static final String CONNECT_DATABASE_TEMPLATE = "'{'\"connectDatabase\":\"{0}\", \"username\":\"{1}\", \"password\":\"{2}\", \"service\":\"{3}\"'}'";
    private static final String PREPARE_STATEMENT_TEMPLATE = "'{'\"prepareStatement\":\"{0}\", \"chunkSize\":{1}'}'";
    private static final String RECONNECT_DATABASE_TEMPLATE = "'{'\"reconnectDatabase\":\"{0}\", \"username\":\"{1}\", \"password\":\"{2}\", \"service\":\"{3}\", \"connectionId\":{4, number, #}, \"listenerId\":{5, number, #}'}'";
    private static final String RECONSTRUCT_STATEMENT_TEMPLATE = "'{'\"reconstructStatement\":{0, number, #}'}'";
    private static final String PUT_TEMPLATE = "'{'\"put\":{0, number, #}'}'";

    private static final String COMMAND_CLOSE_CONNECTION = "{\"closeConnection\":\"closeConnection\"}";
    private static final String COMMAND_CLOSE_STATEMENT = "{\"closeStatement\":\"closeStatement\"}";
    private static final String COMMAND_GET_STATEMENT_ID = "{\"getStatementId\":\"getStatementId\"}";
    private static final String COMMAND_EXECUTE = "{\"execute\":\"execute\"}";
    private static final String COMMAND_FETCH = "{\"fetch\":\"fetch\"}";

    private static final String EVENT_STATEMENT_CLOSED = "{\"statementClosed\":\"statementClosed\"}";
    private static final String EVENT_CONNECTION_CLOSED = "{\"connectionClosed\":\"connectionClosed\"}";
    private static final String EVENT_EXECUTED = "{\"executed\":\"executed\"}";
    private static final String EVENT_PUTTED = "{\"putted\":\"putted\"}";
    private static final String EVENT_STATEMENT_RECONSTRUCTED = "{\"statementReconstructed\":\"statementReconstructed\"}";

    private static final String QUERY_TYPE_IN = "{\"queryTypeIn\":\"queryTypeIn\"}";
    private static final String QUERY_TYPE_OUT = "{\"queryTypeOut\":\"queryTypeOut\"}";

    private final SQSocketConnector socket;

    Messenger(SQSocketConnector socket) {
        this.socket = socket;
    }

    String connect(String database, String user, String password, String service)
            throws IOException, ConnException {
        String connStr = MessageFormat.format(CONNECT_DATABASE_TEMPLATE, database, user, password, service);
        return socket.sendMessage(connStr, true);
    }

    void reconnect(String database, String user, String password, String service, int connection_id, int listener_id)
            throws IOException, ConnException {
        String reconnectStr = MessageFormat.format(RECONNECT_DATABASE_TEMPLATE,
                database, user, password, service, connection_id, listener_id);
        socket.sendMessage(reconnectStr, true);
    }

    void closeConnection() throws IOException, ConnException {
        validateResponse(socket.sendMessage(COMMAND_CLOSE_CONNECTION, true), EVENT_CONNECTION_CLOSED);
    }

    String closeStatement() throws IOException, ConnException {
        return validateResponse(socket.sendMessage(COMMAND_CLOSE_STATEMENT, true), EVENT_STATEMENT_CLOSED);
    }

    String fetch() throws IOException, ConnException {
        return socket.sendMessage(COMMAND_FETCH, true);
    }

    void isStatementReconstructed(int statementId) throws IOException, ConnException {
        validateResponse(socket.sendMessage(MessageFormat.format(RECONSTRUCT_STATEMENT_TEMPLATE, statementId),
                true), EVENT_STATEMENT_RECONSTRUCTED);
    }

    String getStatementId() throws IOException, ConnException {
        return socket.sendMessage(COMMAND_GET_STATEMENT_ID, true);
    }

    void execute() throws IOException, ConnException {
        validateResponse(socket.sendMessage(COMMAND_EXECUTE, true), EVENT_EXECUTED);
    }

    void isPutted() throws IOException, ConnException {
        validateResponse(socket.sendData(null, true), EVENT_PUTTED);
    }

    void put(int row_counter) throws IOException, ConnException {
        socket.sendMessage(MessageFormat.format(PUT_TEMPLATE, row_counter), false);
    }

    String queryTypeInput() throws IOException, ConnException {
        return socket.sendMessage(QUERY_TYPE_IN, true);
    }

    String queryTypeOut() throws IOException, ConnException {
        return socket.sendMessage(QUERY_TYPE_OUT, true);
    }

    private String validateResponse(String response, String expected) throws ConnException {
        if (!response.equals(expected)) {
            throw new ConnException("Expected message: " + expected + " but got " + response);
        }
        return response;
    }
}

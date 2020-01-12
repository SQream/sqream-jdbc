package com.sqream.jdbc.connector.messenger;

import com.sqream.jdbc.connector.ConnException;
import com.sqream.jdbc.connector.SQSocketConnector;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.logging.Level;
import java.util.logging.Logger;

class MessageSender {
    private static final Logger LOGGER = Logger.getLogger(MessageSender.class.getName());

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

    MessageSender(SQSocketConnector socket) {
        this.socket = socket;
    }

    String connect(String database, String user, String password, String service)
            throws IOException, ConnException {
        String connStr = MessageFormat.format(CONNECT_DATABASE_TEMPLATE, database, user, password, service);
        String response = socket.sendMessage(connStr, true);
        LOGGER.log(Level.FINE, MessageFormat.format("Request: [{0}], Response: [{1}]", connStr, response));
        return response;
    }

    void reconnect(String database, String user, String password, String service, int connection_id, int listener_id)
            throws IOException, ConnException {
        String reconnectStr = MessageFormat.format(RECONNECT_DATABASE_TEMPLATE,
                database, user, password, service, connection_id, listener_id);
        String response = socket.sendMessage(reconnectStr, true);
        LOGGER.log(Level.FINE, MessageFormat.format("Request: [{0}], Response: [{1}]", reconnectStr, response));
    }

    void closeConnection() throws IOException, ConnException {
        String response = socket.sendMessage(COMMAND_CLOSE_CONNECTION, true);
        LOGGER.log(Level.FINE, MessageFormat.format(
                "Request: [{0}], Response: [{1}]", COMMAND_CLOSE_CONNECTION, response));
        validateResponse(response, EVENT_CONNECTION_CLOSED);
    }

    String closeStatement() throws IOException, ConnException {
        String response = socket.sendMessage(COMMAND_CLOSE_STATEMENT, true);
        LOGGER.log(Level.FINE, MessageFormat.format(
                "Request: [{0}], Response: [{1}]", COMMAND_CLOSE_STATEMENT, response));
        return validateResponse(response, EVENT_STATEMENT_CLOSED);
    }

    String fetch() throws IOException, ConnException {
        String response = socket.sendMessage(COMMAND_FETCH, true);
        LOGGER.log(Level.FINE, MessageFormat.format(
                "Request: [{0}], Response: [{1}]", COMMAND_FETCH, response));
        return response;
    }

    void isStatementReconstructed(int statementId) throws IOException, ConnException {
        validateResponse(socket.sendMessage(MessageFormat.format(RECONSTRUCT_STATEMENT_TEMPLATE, statementId),
                true), EVENT_STATEMENT_RECONSTRUCTED);
    }

    String getStatementId() throws IOException, ConnException {
        String response = socket.sendMessage(COMMAND_GET_STATEMENT_ID, true);
        LOGGER.log(Level.FINE, MessageFormat.format(
                "Request: [{0}], Response: [{1}]", COMMAND_GET_STATEMENT_ID, response));
        return response;
    }

    void execute() throws IOException, ConnException {
        String response = socket.sendMessage(COMMAND_EXECUTE, true);
        LOGGER.log(Level.FINE, MessageFormat.format(
                "Request: [{0}], Response: [{1}]", COMMAND_EXECUTE, response));
        validateResponse(response, EVENT_EXECUTED);
    }

    void isPutted() throws IOException, ConnException {
        String response = socket.sendData(null, true);
        LOGGER.log(Level.FINE, MessageFormat.format(
                "Request: [{0}], Response: [{1}]", null, response));
        validateResponse(response, EVENT_PUTTED);
    }

    void put(int row_counter) throws IOException, ConnException {
        String message = MessageFormat.format(PUT_TEMPLATE, row_counter);
        socket.sendMessage(message, false);
        LOGGER.log(Level.FINE, message);
    }

    String queryTypeInput() throws IOException, ConnException {
        String response = socket.sendMessage(QUERY_TYPE_IN, true);
        LOGGER.log(Level.FINE, MessageFormat.format("Request: [{0}], Response: [{1}]", QUERY_TYPE_IN, response));
        return response;
    }

    String queryTypeOut() throws IOException, ConnException {
        String response = socket.sendMessage(QUERY_TYPE_OUT, true);
        LOGGER.log(Level.FINE, MessageFormat.format("Request: [{0}], Response: [{1}]", QUERY_TYPE_OUT, response));
        return response;
    }

    private String validateResponse(String response, String expected) throws ConnException {
        if (!response.equals(expected)) {
            throw new ConnException("Expected message: " + expected + " but got " + response);
        }
        return response;
    }
}

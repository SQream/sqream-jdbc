package com.sqream.jdbc.connector.messenger;

import com.eclipsesource.json.Json;
import com.eclipsesource.json.JsonObject;
import com.eclipsesource.json.ParseException;
import com.eclipsesource.json.WriterConfig;
import com.sqream.jdbc.connector.*;
import com.sqream.jdbc.connector.socket.SQSocketConnector;

import java.nio.ByteBuffer;
import java.text.MessageFormat;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class MessengerImpl implements Messenger {
    private static final Logger LOGGER = Logger.getLogger(MessengerImpl.class.getName());

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

    private SQSocketConnector socket;
    private JsonParser jsonParser;

    private MessengerImpl(SQSocketConnector socket) {
        this.socket = socket;
        this.jsonParser = new JsonParser();
    }

    public static Messenger getInstance(SQSocketConnector socket) {
        return new MessengerImpl(socket);
    }

    @Override
    public FetchMetadataDto fetch() throws ConnException {
        String response = sendMessage(COMMAND_FETCH, true);
        LOGGER.log(Level.FINE, MessageFormat.format(
                "Request: [{0}], Response: [{1}]", COMMAND_FETCH, response));
        return jsonParser.toFetchMetadata(response);
    }

    @Override
    public ConnectionStateDto connect(String database, String user, String password, String service)
            throws ConnException {
        String connStr = MessageFormat.format(CONNECT_DATABASE_TEMPLATE, database, user, password, service);
        String response = sendMessage(connStr, true);
        LOGGER.log(Level.FINE, MessageFormat.format("Request: [{0}], Response: [{1}]", connStr, response));
        return jsonParser.toConnectionState(response);
    }

    @Override
    public int openStatement() throws ConnException {
        String response = sendMessage(COMMAND_GET_STATEMENT_ID, true);
        LOGGER.log(Level.FINE, MessageFormat.format(
                "Request: [{0}], Response: [{1}]", COMMAND_GET_STATEMENT_ID, response));
        return jsonParser.toStatementId(response);
    }

    @Override
    public void isStatementReconstructed(int statementId) throws ConnException {
        validateResponse(sendMessage(MessageFormat.format(RECONSTRUCT_STATEMENT_TEMPLATE, statementId),
                true), EVENT_STATEMENT_RECONSTRUCTED);
    }

    @Override
    public List<ColumnMetadataDto> queryTypeInput() throws ConnException {
        String response = sendMessage(QUERY_TYPE_IN, true);
        LOGGER.log(Level.FINE, MessageFormat.format("Request: [{0}], Response: [{1}]", QUERY_TYPE_IN, response));
        return jsonParser.toQueryTypeInput(response);
    }

    @Override
    public List<ColumnMetadataDto> queryTypeOut() throws ConnException {
        String response = sendMessage(QUERY_TYPE_OUT, true);
        LOGGER.log(Level.FINE, MessageFormat.format("Request: [{0}], Response: [{1}]", QUERY_TYPE_OUT, response));
        return jsonParser.toQueryTypeOut(response);
    }

    @Override
    public void closeStatement() throws ConnException {
        String response = sendMessage(COMMAND_CLOSE_STATEMENT, true);
        LOGGER.log(Level.FINE, MessageFormat.format(
                "Request: [{0}], Response: [{1}]", COMMAND_CLOSE_STATEMENT, response));
        validateResponse(response, EVENT_STATEMENT_CLOSED);
    }

    @Override
    public void closeConnection() throws ConnException {
        String response = sendMessage(COMMAND_CLOSE_CONNECTION, true);
        LOGGER.log(Level.FINE, MessageFormat.format(
                "Request: [{0}], Response: [{1}]", COMMAND_CLOSE_CONNECTION, response));
        validateResponse(response, EVENT_CONNECTION_CLOSED);
    }

    @Override
    public void put(int rowCounter) throws ConnException {
        String message = MessageFormat.format(PUT_TEMPLATE, rowCounter);
        sendMessage(message, false);
        LOGGER.log(Level.FINE, message);
    }

    @Override
    public void isPutted() throws ConnException {
        String response = socket.sendData(null, true);
        LOGGER.log(Level.FINE, MessageFormat.format(
                "Request: [{0}], Response: [{1}]", null, response));
        validateResponse(response, EVENT_PUTTED);
    }

    @Override
    public void execute() throws ConnException {
        String response = sendMessage(COMMAND_EXECUTE, true);
        LOGGER.log(Level.FINE, MessageFormat.format(
                "Request: [{0}], Response: [{1}]", COMMAND_EXECUTE, response));
        validateResponse(response, EVENT_EXECUTED);
    }

    @Override
    public void reconnect(String database, String user, String password, String service, int connectionId, int listenerId) throws ConnException {
        String reconnectStr = MessageFormat.format(RECONNECT_DATABASE_TEMPLATE,
                database, user, password, service, connectionId, listenerId);
        String response = sendMessage(reconnectStr, true);
        LOGGER.log(Level.FINE, MessageFormat.format("Request: [{0}], Response: [{1}]", reconnectStr, response));
    }

    @Override
    public StatementStateDto prepareStatement(String statement, int chunkSize) throws ConnException {
        JsonObject prepare_jsonify;
        try {
            prepare_jsonify = Json.object()
                    .add("prepareStatement", statement)
                    .add("chunkSize", chunkSize);
        } catch(ParseException e) {
            throw new ConnException (MessageFormat.format("Could not parse the statement for PrepareStatement: [{0}]", statement));
        }
        String prepareStr = prepare_jsonify.toString(WriterConfig.MINIMAL);

        return jsonParser.toStatementState(sendMessage(prepareStr, true));
    }

    private String validateResponse(String response, String expected) throws ConnException {
        if (!response.equals(expected)) {
            throw new ConnException("Expected message: " + expected + " but got " + response);
        }
        return response;
    }

    private String sendMessage(String message, boolean getResponse) throws ConnException {
        byte[] messageBytes = message.getBytes();
        ByteBuffer messageBuffer = socket.generateHeaderedBuffer(messageBytes.length, true);
        messageBuffer.put(messageBytes);

        return socket.sendData(messageBuffer, getResponse);
    }
}

package com.sqream.jdbc.connector.messenger;

import com.eclipsesource.json.Json;
import com.eclipsesource.json.JsonObject;
import com.eclipsesource.json.ParseException;
import com.eclipsesource.json.WriterConfig;
import com.sqream.jdbc.connector.*;

import java.io.IOException;
import java.util.List;

public class MessengerImpl implements Messenger {

    private SQSocketConnector socket;
    private JsonParser jsonParser;
    private MessageSender messageSender;

    public MessengerImpl(SQSocketConnector socket) {
        this.socket = socket;
        this.jsonParser = new JsonParser();
        this.messageSender = new MessageSender(socket);
    }

    @Override
    public FetchMetadataDto fetch() throws IOException, ConnException {
        return jsonParser.toFetchMetadata(messageSender.fetch());
    }

    @Override
    public ConnectionStateDto connect(String database, String user, String password, String service)
            throws IOException, ConnException {
        return jsonParser.toConnectionState(messageSender.connect(database, user, password, service));
    }

    @Override
    public int openStatement() throws IOException, ConnException {
        return jsonParser.toStatementId(messageSender.getStatementId());
    }

    @Override
    public void isStatementReconstructed(int statementId) throws IOException, ConnException {
        messageSender.isStatementReconstructed(statementId);
    }

    @Override
    public List<ColumnMetadataDto> queryTypeInput() throws IOException, ConnException {
        return jsonParser.toQueryTypeInput(messageSender.queryTypeInput());
    }

    @Override
    public List<ColumnMetadataDto> queryTypeOut() throws IOException, ConnException {
        return jsonParser.toQueryTypeOut(messageSender.queryTypeOut());
    }

    @Override
    public void closeStatement() throws IOException, ConnException {
        messageSender.closeStatement();
    }

    @Override
    public void closeConnection() throws IOException, ConnException {
        messageSender.closeConnection();
    }

    @Override
    public void put(int rowCounter) throws IOException, ConnException {
        messageSender.put(rowCounter);
    }

    @Override
    public void isPutted() throws IOException, ConnException {
        messageSender.isPutted();
    }

    @Override
    public void execute() throws IOException, ConnException {
        messageSender.execute();
    }

    @Override
    public void reconnect(String database, String user, String password, String service, int connectionId, int listenerId) throws IOException, ConnException {
        messageSender.reconnect(database, user, password, service, connectionId, listenerId);
    }

    @Override
    public StatementStateDto prepareStatement(String statement, int chunkSize) throws ConnException, IOException {
        JsonObject prepare_jsonify;
        try {
            prepare_jsonify = Json.object()
                    .add("prepareStatement", statement)
                    .add("chunkSize", chunkSize);
        } catch(ParseException e) {
            throw new ConnException ("Could not parse the statement for PrepareStatement");
        }
        String prepareStr = prepare_jsonify.toString(WriterConfig.MINIMAL);

        return jsonParser.toStatementState(socket.sendMessage(prepareStr, true));
    }
}

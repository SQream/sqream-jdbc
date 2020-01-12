package com.sqream.jdbc.connector.messenger;

import com.sqream.jdbc.connector.*;

import java.io.IOException;
import java.util.List;

public interface Messenger {

    FetchMetadataDto fetch() throws IOException, ConnException;

    ConnectionStateDto connect(String database, String user, String password, String service) throws IOException, ConnException;

    void reconnect(String database, String user, String password, String service, int connectionId, int listenerId) throws IOException, ConnException;

    int openStatement() throws IOException, ConnException;

    void isStatementReconstructed(int statementId) throws IOException, ConnException;

    void execute() throws IOException, ConnException;

    List<ColumnMetadataDto> queryTypeInput() throws IOException, ConnException;

    List<ColumnMetadataDto> queryTypeOut() throws IOException, ConnException;

    void closeStatement() throws IOException, ConnException;

    void closeConnection() throws IOException, ConnException;

    void put(int rowCounter) throws IOException, ConnException;

    void isPutted() throws IOException, ConnException;

    StatementStateDto prepareStatement(String statement, int chunkSize) throws ConnException, IOException;
}

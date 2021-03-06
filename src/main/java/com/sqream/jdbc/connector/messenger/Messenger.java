package com.sqream.jdbc.connector.messenger;

import com.sqream.jdbc.connector.*;

import java.nio.ByteBuffer;
import java.util.List;

public interface Messenger {

    FetchMetadataDto fetch() throws ConnException;

    ConnectionStateDto connect(String database, String user, String password, String service) throws ConnException;

    void reconnect(String ip, int port, boolean useSsl, String database, String user, String password, String service,
                   int connectionId, int listenerId) throws ConnException;

    int openStatement() throws ConnException;

    void isStatementReconstructed(int statementId) throws ConnException;

    void execute() throws ConnException;

    List<ColumnMetadataDto> queryTypeInput() throws ConnException;

    List<ColumnMetadataDto> queryTypeOut() throws ConnException;

    void closeStatement() throws ConnException;

    void closeConnection() throws ConnException;

    void put(int rowCounter) throws ConnException;

    void ping() throws ConnException;

    void isPutted() throws ConnException;

    StatementStateDto prepareStatement(String statement, int chunkSize) throws ConnException;

    void sendBinaryHeader(long dataLength) throws ConnException;

    void sendBinaryData(ByteBuffer buffer) throws ConnException;

    void parseHeader() throws ConnException;

    void fetchBinaryData(ByteBuffer target, int size) throws ConnException;
}

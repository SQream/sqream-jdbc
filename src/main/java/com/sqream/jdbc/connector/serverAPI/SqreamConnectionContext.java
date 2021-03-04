package com.sqream.jdbc.connector.serverAPI;

import com.sqream.jdbc.ConnectionParams;
import com.sqream.jdbc.connector.ColumnMetadataDto;
import com.sqream.jdbc.connector.ConnectionStateDto;
import com.sqream.jdbc.connector.heartbeat.HeartBeatService;
import com.sqream.jdbc.connector.messenger.Messenger;
import com.sqream.jdbc.connector.serverAPI.enums.StatementPhase;

import java.util.List;

public class SqreamConnectionContext {

    private Integer statementId;
    private Messenger messenger;
    private ConnectionParams connParams;
    private ConnectionStateDto connState;
    private String query;
    private int chunkSize;
    private List<ColumnMetadataDto> columnsMetadata;
    private StatementPhase statementPhase;

    public SqreamConnectionContext(ConnectionStateDto connState,
                                   ConnectionParams connParams,
                                   int chunkSize,
                                   Messenger messenger) {
        this.connState = connState;
        this.connParams = connParams;
        this.chunkSize = chunkSize;
        this.messenger = messenger;
    }

    public ConnectionStateDto getConnState() {
        return connState;
    }

    public void setConnState(ConnectionStateDto connState) {
        this.connState = connState;
    }

    public ConnectionParams getConnParams() {
        return connParams;
    }

    public void setConnParams(ConnectionParams connParams) {
        this.connParams = connParams;
    }

    public Messenger getMessenger() {
        return messenger;
    }

    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    public Integer getStatementId() {
        return statementId;
    }

    public void setStatementId(Integer statementId) {
        this.statementId = statementId;
    }

    public int getChunkSize() {
        return chunkSize;
    }

    public void setChunkSize(int chunkSize) {
        this.chunkSize = chunkSize;
    }

    public List<ColumnMetadataDto> getColumnsMetadata() {
        return columnsMetadata;
    }

    public void setColumnsMetadata(List<ColumnMetadataDto> columnsMetadata) {
        this.columnsMetadata = columnsMetadata;
    }

    public StatementPhase getStatementPhase() {
        return statementPhase;
    }

    public void setStatementPhase(StatementPhase statementState) {
        this.statementPhase = statementState;
    }

}

package com.sqream.jdbc.connector.serverAPI.Statement;

import com.sqream.jdbc.connector.ColumnMetadataDto;
import com.sqream.jdbc.connector.ConnException;
import com.sqream.jdbc.connector.enums.StatementType;
import com.sqream.jdbc.connector.serverAPI.SqreamConnectionContext;
import com.sqream.jdbc.connector.serverAPI.enums.StatementPhase;

import java.util.List;

import static com.sqream.jdbc.connector.enums.StatementType.*;
import static com.sqream.jdbc.connector.serverAPI.enums.StatementPhase.PREPARED;

public class SqreamPreparedStatementImpl extends BasesProtocolPhase implements SqreamPreparedStatement {

    public SqreamPreparedStatementImpl(SqreamConnectionContext context) {
        super(context);
    }

    @Override
    public StatementPhase getStatementPhase() {
        return PREPARED;
    }

    @Override
    public SqreamExecutedStatement execute() {
        try {
            context.getPingService().start();
            context.getMessenger().execute();
            context.getPingService().stop();
            List<ColumnMetadataDto> columnsMetadata = context.getMessenger().queryTypeInput();
            StatementType statementType;
            if (!columnsMetadata.isEmpty()) {
                statementType = NETWORK_INSERT;
            } else {
                columnsMetadata = context.getMessenger().queryTypeOut();
                statementType = columnsMetadata.isEmpty() ? NON_QUERY : QUERY;
            }
            context.setColumnsMetadata(columnsMetadata);
            return new SqreamExecutedStatementImpl(context, statementType);
        } catch (ConnException e) { //TODO needs refactor to throw correct exception Alex K 11.11.2020
            close();
            throw new RuntimeException(e);
        }
    }
}

package com.sqream.jdbc.connector.serverAPI.Statement;

import com.sqream.jdbc.connector.BlockDto;
import com.sqream.jdbc.connector.TableMetadata;
import com.sqream.jdbc.connector.enums.StatementType;

public interface SqreamExecutedStatement extends CloseableSqreamStatement {
    StatementType getType();
    BlockDto fetch();
    void put(BlockDto block);
    TableMetadata getMeta();
}

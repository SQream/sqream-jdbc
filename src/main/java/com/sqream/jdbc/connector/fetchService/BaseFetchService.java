package com.sqream.jdbc.connector.fetchService;

import com.sqream.jdbc.connector.BlockDto;
import com.sqream.jdbc.connector.ConnException;
import com.sqream.jdbc.connector.serverAPI.Statement.SqreamExecutedStatement;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;

public abstract class BaseFetchService implements FetchService{
    private SqreamExecutedStatement sqreamExecutedStatement;
    protected List<BlockDto> fetchedBlocks = new ArrayList<>();

    BaseFetchService(SqreamExecutedStatement sqreamExecutedStatement) {
        this.sqreamExecutedStatement = sqreamExecutedStatement;
    }

    protected int fetch() {
        BlockDto fetchedBlock = sqreamExecutedStatement.fetch();
        fetchedBlocks.add(fetchedBlock);
        return fetchedBlock.getFillSize();
    }

    protected void validateLimit(int limit) throws ConnException {
        if (limit < 0) {
            throw new ConnException(MessageFormat.format("Limit [{0}] should be non-negative", limit));
        }
    }
}

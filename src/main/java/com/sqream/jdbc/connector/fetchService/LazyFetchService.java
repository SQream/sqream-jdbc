package com.sqream.jdbc.connector.fetchService;

import com.sqream.jdbc.connector.BlockDto;
import com.sqream.jdbc.connector.ConnException;
import com.sqream.jdbc.connector.serverAPI.Statement.SqreamExecutedStatement;

import java.text.MessageFormat;
import java.util.logging.Level;
import java.util.logging.Logger;

public class LazyFetchService extends BaseFetchService implements FetchService {
    private static final Logger LOGGER = Logger.getLogger(LazyFetchService.class.getName());

    private int fetchSize;
    private int maxRows = 0;
    private int totalFetched = 0;
    private boolean closed = false;

    private LazyFetchService(SqreamExecutedStatement sqreamExecutedStatement, int fetchSize) {
        super(sqreamExecutedStatement);
        this.fetchSize = fetchSize;
    }

    public static LazyFetchService getInstance(SqreamExecutedStatement sqreamExecutedStatement,
                                               int fetchSize) {
        return new LazyFetchService(sqreamExecutedStatement, fetchSize);
    }

    @Override
    public void process(int rowAmount) throws ConnException {
        LOGGER.log(Level.FINE, MessageFormat.format(
                "Process: rowAmount=[{0}], fetchSize=[{1}]", rowAmount, fetchSize));
        validateLimit(rowAmount);
        maxRows = rowAmount;
    }

    @Override
    public BlockDto getBlock() throws ConnException {
        int fetchedInCurrentSession = 0;
        if (fetchedBlocks.size() == 0 && !closed) {
            while (maxRows == 0 || totalFetched < maxRows) {
                int newRowsFetched = fetch();
                fetchedInCurrentSession += newRowsFetched;
                if (newRowsFetched == 0) {
                    closed = true;
                    break;
                }
                totalFetched += newRowsFetched;
                if (fetchedInCurrentSession >= fetchSize) {
                    break;
                }
            }
        }
        return fetchedBlocks.size() > 0 ? fetchedBlocks.remove(0) : null;
    }

    @Override
    public boolean isClosed() {
        return closed;
    }
}

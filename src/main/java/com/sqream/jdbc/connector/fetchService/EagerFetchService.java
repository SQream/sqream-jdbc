package com.sqream.jdbc.connector.fetchService;

import com.sqream.jdbc.connector.*;
import com.sqream.jdbc.connector.serverAPI.Statement.SqreamExecutedStatement;

import java.text.MessageFormat;
import java.util.logging.Level;
import java.util.logging.Logger;

public class EagerFetchService extends BaseFetchService implements FetchService {
    private static final Logger LOGGER = Logger.getLogger(EagerFetchService.class.getName());

    private EagerFetchService(SqreamExecutedStatement sqreamExecutedStatement) {
        super(sqreamExecutedStatement);
    }

    public static EagerFetchService getInstance(SqreamExecutedStatement sqreamExecutedStatement) {
        return new EagerFetchService(sqreamExecutedStatement);
    }

    @Override
    public void process(int limit) throws ConnException {
        LOGGER.log(Level.FINE, MessageFormat.format("Process: limit=[{0}]", limit));
        validateLimit(limit);

        int totalFetched = 0;
        while (limit == 0 || totalFetched < limit) {
            int newRowsFetched = fetch();
            if (newRowsFetched ==0) {
                break;
            }
            totalFetched += newRowsFetched;
        }
    }

    @Override
    public BlockDto getBlock() {
        return fetchedBlocks.size() > 0 ? fetchedBlocks.remove(0) : null;
    }

    @Override
    public boolean isClosed() {
        return true;
    }
}

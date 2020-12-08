package com.sqream.jdbc.connector.fetchService;

import com.sqream.jdbc.connector.serverAPI.Statement.SqreamExecutedStatement;

public class FetchServiceFactory {
    private static final int UNLIMITED_FETCH = 0;

    public static FetchService getService(SqreamExecutedStatement sqreamExecutedStatement,
                                          int fetchSize) {

        if (fetchSize == UNLIMITED_FETCH) {
            return EagerFetchService.getInstance(sqreamExecutedStatement);
        } else {
            return LazyFetchService.getInstance(sqreamExecutedStatement, fetchSize);
        }
    }
}

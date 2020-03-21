package com.sqream.jdbc.connector.fetchService;

import com.sqream.jdbc.connector.TableMetadata;
import com.sqream.jdbc.connector.messenger.Messenger;
import com.sqream.jdbc.connector.socket.SQSocketConnector;

public class FetchServiceFactory {
    private static final int UNLIMITED_FETCH = 0;

    public static FetchService getService(SQSocketConnector socket,
                                          Messenger messenger,
                                          TableMetadata metadata,
                                          int fetchSize) {

        if (fetchSize == UNLIMITED_FETCH) {
            return EagerFetchService.getInstance(socket, messenger, metadata);
        } else {
            return LazyFetchService.getInstance(socket, messenger, metadata, fetchSize);
        }
    }
}

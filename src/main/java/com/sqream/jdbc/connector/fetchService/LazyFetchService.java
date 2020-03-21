package com.sqream.jdbc.connector.fetchService;

import com.sqream.jdbc.connector.BlockDto;
import com.sqream.jdbc.connector.ConnException;
import com.sqream.jdbc.connector.TableMetadata;
import com.sqream.jdbc.connector.messenger.Messenger;
import com.sqream.jdbc.connector.socket.SQSocketConnector;

public class LazyFetchService extends BaseFetchService implements FetchService {

    private LazyFetchService(SQSocketConnector socket, Messenger messenger, TableMetadata metadata) {
        super(socket, messenger, metadata);
    }

    public static LazyFetchService getInstance(SQSocketConnector socket, Messenger messenger, TableMetadata metadata) {
        return new LazyFetchService(socket, messenger, metadata);
    }

    @Override
    public void process(int rowAmount) throws ConnException {
        throw new UnsupportedOperationException();
    }

    @Override
    public BlockDto getBlock() {
        throw new UnsupportedOperationException();
    }
}

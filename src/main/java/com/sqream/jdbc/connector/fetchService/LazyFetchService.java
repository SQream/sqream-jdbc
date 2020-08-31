package com.sqream.jdbc.connector.fetchService;

import com.sqream.jdbc.connector.BlockDto;
import com.sqream.jdbc.connector.ConnException;
import com.sqream.jdbc.connector.TableMetadata;
import com.sqream.jdbc.connector.messenger.Messenger;
import com.sqream.jdbc.connector.socket.SQSocketConnector;

import java.text.MessageFormat;
import java.util.logging.Level;
import java.util.logging.Logger;

public class LazyFetchService extends BaseFetchService implements FetchService {
    private static final Logger LOGGER = Logger.getLogger(LazyFetchService.class.getName());

    private int fetchSize;
    private int maxRows = 0;
    private int totalFetched = 0;
    private boolean closed = false;

    private LazyFetchService(SQSocketConnector socket, Messenger messenger, TableMetadata metadata, int fetchSize) {
        super(socket, messenger, metadata);
        this.fetchSize = fetchSize;
    }

    public static LazyFetchService getInstance(SQSocketConnector socket,
                                               Messenger messenger,
                                               TableMetadata metadata,
                                               int fetchSize) {
        return new LazyFetchService(socket, messenger, metadata, fetchSize);
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

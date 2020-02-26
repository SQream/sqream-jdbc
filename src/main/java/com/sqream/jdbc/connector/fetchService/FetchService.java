package com.sqream.jdbc.connector.fetchService;

import com.sqream.jdbc.connector.*;
import com.sqream.jdbc.connector.messenger.Messenger;

import javax.script.ScriptException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class FetchService {
    private static final Logger LOGGER = Logger.getLogger(FetchService.class.getName());

    private SQSocketConnector socket;
    private Messenger messenger;
    private TableMetadata metadata;

    private FetchService(SQSocketConnector socket, Messenger messenger, TableMetadata metadata) {
        this.socket = socket;
        this.messenger = messenger;
        this.metadata = metadata;
    }

    public static FetchService getInstance(SQSocketConnector socket, Messenger messenger, TableMetadata metadata) {
        return new FetchService(socket, messenger, metadata);
    }

    public List<BlockDto> process(int rowAmount) throws IOException, ScriptException, ConnException {
        LOGGER.log(Level.FINE, MessageFormat.format("Fetch [{0}]", rowAmount));

        List<BlockDto> resultBlocks = new ArrayList<>();

        int total_fetched = 0;
        int new_rows_fetched;

        if (rowAmount < -1) {
            throw new ConnException("row_amount should be positive, got " + rowAmount);
        }
        if (rowAmount == -1) {
            // Place for adding logic for previos fetching behavior - per
            // requirement fetch
        }
        else {  // positive row amount
            while (rowAmount == 0 || total_fetched < rowAmount) {
                new_rows_fetched = fetch(resultBlocks);
                if (new_rows_fetched ==0)
                    break;
                total_fetched += new_rows_fetched;
            }
        }
        return resultBlocks;
    }

    private int fetch(List<BlockDto> resultList) throws IOException, ScriptException, ConnException {
        /* Request and get data from SQream following a SELECT query */

        // Send fetch request and get metadata on data to be received

        FetchMetadataDto fetchMeta = messenger.fetch();

        if (fetchMeta.getNewRowsFetched() == 0) {
            return fetchMeta.getNewRowsFetched();
        }
        // Initiate storage columns using the "colSzs" returned by SQream
        // All buffers in a single array to use SocketChannel's read(ByteBuffer[] dsts)
        ByteBuffer[] fetch_buffers = new ByteBuffer[fetchMeta.colAmount()];

        for (int i=0; i < fetchMeta.colAmount(); i++) {
            fetch_buffers[i] = ByteBuffer.allocateDirect(fetchMeta.getSizeByIndex(i)).order(ByteOrder.LITTLE_ENDIAN);
        }

        // Initial naive implememntation - Get all socket data in advance
        int bytes_read = socket.getParseHeader();   // Get header out of the way
        for (ByteBuffer fetched : fetch_buffers) {
            socket.readData(fetched, fetched.capacity());
            //Arrays.stream(fetch_buffers).forEach(fetched -> fetched.flip());
        }

        // Add buffers to buffer list
        resultList.add(parse(fetch_buffers, metadata, fetchMeta.getNewRowsFetched()));

        return fetchMeta.getNewRowsFetched();  // counter nullified by next()
    }

    private BlockDto parse(ByteBuffer[] fetchBuffers, TableMetadata metadata, int rowsFetched) {
        ByteBuffer[] dataColumns = new ByteBuffer[metadata.getRowLength()];
        ByteBuffer[] nullColumns = new ByteBuffer[metadata.getRowLength()];
        ByteBuffer[] nvarcLenColumns = new ByteBuffer[metadata.getRowLength()];
        for (int idx = 0, buf_idx = 0; idx < metadata.getRowLength(); idx++, buf_idx++) {
            if (metadata.isNullable(idx)) {
                nullColumns[idx] = fetchBuffers[buf_idx];
                buf_idx++;
            } else {
                nullColumns[idx] = null;
            }
            if (metadata.isTruVarchar(idx)) {
                nvarcLenColumns[idx] = fetchBuffers[buf_idx];
                buf_idx++;
            } else {
                nvarcLenColumns[idx] = null;
            }
            dataColumns[idx] = fetchBuffers[buf_idx];
        }
        BlockDto resultBlock = new BlockDto(dataColumns, nullColumns, nvarcLenColumns, rowsFetched);
        resultBlock.setFillSize(rowsFetched);
        return resultBlock;
    }
}

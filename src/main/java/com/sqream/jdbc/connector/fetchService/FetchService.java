package com.sqream.jdbc.connector.fetchService;

import com.sqream.jdbc.connector.*;
import com.sqream.jdbc.connector.messenger.Messenger;
import com.sqream.jdbc.connector.socket.SQSocketConnector;

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
    private List<BlockDto> fetchedBlocks = new ArrayList<>();

    private FetchService(SQSocketConnector socket, Messenger messenger, TableMetadata metadata) {
        this.socket = socket;
        this.messenger = messenger;
        this.metadata = metadata;
    }

    public static FetchService getInstance(SQSocketConnector socket, Messenger messenger, TableMetadata metadata) {
        return new FetchService(socket, messenger, metadata);
    }

    public void process(int rowAmount) throws ConnException {
        LOGGER.log(Level.FINE, MessageFormat.format("Fetch [{0}]", rowAmount));

        if (rowAmount < 0) {
            throw new ConnException(MessageFormat.format("Row amount [{0}] should be positive", rowAmount));
        }

        int totalFetched = 0;
        int newRowsFetched;

        while (rowAmount == 0 || totalFetched < rowAmount) {
            newRowsFetched = fetch();
            if (newRowsFetched ==0)
                break;
            totalFetched += newRowsFetched;
        }
    }

    public BlockDto getBlock() {
        return fetchedBlocks.size() > 0 ? fetchedBlocks.remove(0) : null;
    }

    private int fetch() throws ConnException {

        FetchMetadataDto fetchMeta = messenger.fetch();

        if (fetchMeta.getNewRowsFetched() == 0) {
            return fetchMeta.getNewRowsFetched();
        }

        ByteBuffer[] fetch_buffers = new ByteBuffer[fetchMeta.colAmount()];

        for (int i=0; i < fetchMeta.colAmount(); i++) {
            fetch_buffers[i] = ByteBuffer.allocateDirect(fetchMeta.getSizeByIndex(i)).order(ByteOrder.LITTLE_ENDIAN);
        }

        int bytes_read = socket.parseHeader();   // Get header out of the way
        for (ByteBuffer fetchBuffer : fetch_buffers) {
            socket.readData(fetchBuffer, fetchBuffer.capacity());
        }

        fetchedBlocks.add(parse(fetch_buffers, metadata, fetchMeta.getNewRowsFetched()));

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

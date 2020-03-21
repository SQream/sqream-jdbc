package com.sqream.jdbc.connector.fetchService;

import com.sqream.jdbc.connector.BlockDto;
import com.sqream.jdbc.connector.ConnException;
import com.sqream.jdbc.connector.FetchMetadataDto;
import com.sqream.jdbc.connector.TableMetadata;
import com.sqream.jdbc.connector.messenger.Messenger;
import com.sqream.jdbc.connector.socket.SQSocketConnector;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;

public abstract class BaseFetchService implements FetchService{
    private SQSocketConnector socket;
    private Messenger messenger;
    private TableMetadata metadata;
    protected List<BlockDto> fetchedBlocks = new ArrayList<>();

    BaseFetchService(SQSocketConnector socket, Messenger messenger, TableMetadata metadata) {
        this.socket = socket;
        this.messenger = messenger;
        this.metadata = metadata;
    }

    protected int fetch() throws ConnException {

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

    protected void validateRowAmount(int rowAmount) throws ConnException {
        if (rowAmount < 0) {
            throw new ConnException(MessageFormat.format("Row amount [{0}] should be positive", rowAmount));
        }
    }
}

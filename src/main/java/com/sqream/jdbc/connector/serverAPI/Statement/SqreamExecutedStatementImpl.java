package com.sqream.jdbc.connector.serverAPI.Statement;

import com.sqream.jdbc.connector.BlockDto;
import com.sqream.jdbc.connector.ConnException;
import com.sqream.jdbc.connector.FetchMetadataDto;
import com.sqream.jdbc.connector.TableMetadata;
import com.sqream.jdbc.connector.enums.StatementType;
import com.sqream.jdbc.connector.messenger.Messenger;
import com.sqream.jdbc.connector.socket.SQSocketConnector;
import com.sqream.jdbc.connector.serverAPI.SqreamConnectionContext;
import com.sqream.jdbc.connector.serverAPI.enums.StatementPhase;
import com.sqream.jdbc.utils.Utils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.text.MessageFormat;

import static com.sqream.jdbc.connector.enums.StatementType.NETWORK_INSERT;
import static com.sqream.jdbc.connector.enums.StatementType.QUERY;

public class SqreamExecutedStatementImpl extends BasesProtocolPhase implements SqreamExecutedStatement {
    private StatementType type;
    TableMetadata metadata;

    public SqreamExecutedStatementImpl(SqreamConnectionContext context, StatementType type) {
        super(context);
        this.type = type;
        this.metadata = createTableMetadata();
    }

    @Override
    protected StatementPhase getStatementPhase() {
        return StatementPhase.EXECUTED;
    }

    @Override
    public StatementType getType() {
        return this.type;
    }

    @Override
    public BlockDto fetch() {
        validateFetch();
        return fetchChunk();
    }

    @Override
    public void put(BlockDto block) {
        validatePut();
        try {
            Messenger messenger = context.getMessenger();
            messenger.put(block.getFillSize());
            messenger.sendBinaryHeader(Utils.totalLengthForHeader(metadata, block));
            // Send available columns
            sendDataToSocket(metadata, block);
            messenger.isPutted();
        } catch (ConnException e) { //TODO needs refactor to throw correct exception Alex K 11.11.2020
            close();
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public TableMetadata getMeta() {
        return metadata;
    }

    private void validateFetch() {
        if (type != QUERY) {
            close();
            throw new IllegalStateException(MessageFormat.format(
                    "Statement [{0}] is not a select statement.", context.getQuery()));
        }
    }

    private void validatePut() {
        if (type != NETWORK_INSERT) {
            close();
            throw new IllegalStateException(MessageFormat.format(
                    "Statement [{0}] is not an insert statement.", context.getQuery()));
        }
    }

    private BlockDto fetchChunk() {
        FetchMetadataDto fetchMeta = null;
        try {
            Messenger messenger = context.getMessenger();
            context.getPingService().start();
            fetchMeta = messenger.fetch();
            context.getPingService().stop();
            if (fetchMeta.getNewRowsFetched() == 0) {
                BlockDto emptyBlock = new BlockDto(new ByteBuffer[0], new ByteBuffer[0], new ByteBuffer[0], 0);
                emptyBlock.setFillSize(0);
                return emptyBlock;
            }

            ByteBuffer[] fetch_buffers = new ByteBuffer[fetchMeta.colAmount()];

            for (int i=0; i < fetchMeta.colAmount(); i++) {
                fetch_buffers[i] = ByteBuffer.allocateDirect(fetchMeta.getSizeByIndex(i)).order(ByteOrder.LITTLE_ENDIAN);
            }

            context.getMessenger().parseHeader();   // Get header out of the way
            for (ByteBuffer fetchBuffer : fetch_buffers) {
                messenger.fetchBinaryData(fetchBuffer, fetchBuffer.capacity());
            }

            return parse(fetch_buffers, metadata, fetchMeta.getNewRowsFetched());
        } catch (ConnException e) { //TODO needs refactor to throw correct exception Alex K 11.11.2020
            close();
            throw new RuntimeException(e);
        }
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

    private TableMetadata createTableMetadata() {
        return TableMetadata.builder()
                .rowLength(context.getColumnsMetadata().size())
                .fromColumnsMetadata(context.getColumnsMetadata())
                .statementType(type)
                .build();
    }

    private void sendDataToSocket(TableMetadata tableMetadata, BlockDto block)
            throws IOException, ConnException {
        Messenger messenger = context.getMessenger();

        for(int idx=0; idx < tableMetadata.getRowLength(); idx++) {
            if(tableMetadata.isNullable(idx)) {
                messenger.sendBinaryData((ByteBuffer) block.getNullBuffers()[idx].position(block.getFillSize()));
            }
            if(tableMetadata.isTruVarchar(idx)) {
                messenger.sendBinaryData(block.getNvarcLenBuffers()[idx]);
            }
            messenger.sendBinaryData(block.getDataBuffers()[idx]);
        }
    }
}

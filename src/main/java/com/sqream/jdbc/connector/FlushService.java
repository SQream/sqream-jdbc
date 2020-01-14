package com.sqream.jdbc.connector;

import com.sqream.jdbc.connector.messenger.Messenger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.MessageFormat;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class FlushService {
    private static final Logger LOGGER = Logger.getLogger(FlushService.class.getName());

    private SQSocketConnector socket;
    private Messenger messenger;
    private ExecutorService executorService = Executors.newSingleThreadExecutor();

    private FlushService(SQSocketConnector socket, Messenger messenger) {
        this.socket = socket;
        this.messenger = messenger;
    }

    public static FlushService getInstance(SQSocketConnector socket, Messenger messenger) {
        return  new FlushService(socket, messenger);
    }

    public BlockDto process(int rowLength, int rowCounter, TableMetadata metadata,
                        BlockDto block, int totalLengthForHeader, ByteBufferPool byteBufferPool, boolean async) {
        if (async) {
            if (executorService.isShutdown()) {
                executorService = Executors.newSingleThreadExecutor();
            }

            BlockDto blockForFlush = new BlockDto(block.getDataBuffers(), block.getNullBuffers(), block.getNvarcLenBuffers());

            BlockDto blockFromPool = null;
            try {
                blockFromPool = byteBufferPool.getBlock();
            } catch (InterruptedException e) {
                throw new RuntimeException("Interrupted when getting block from byteBuffer pool", e);
            }

            executorService.submit(() -> {
                try {
                    flush(rowLength,
                            rowCounter,
                            metadata,
                            blockForFlush,
                            totalLengthForHeader);

                    byteBufferPool.releaseBlock(blockForFlush);
                } catch (Exception e) {
                    throw new RuntimeException("Exception when flush data", e);
                }
            });
            return blockFromPool;
        } else {
            try {
                flush(rowLength, rowCounter, metadata, block, totalLengthForHeader);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return block;
        }
    }

    public void awaitTermination() {
        if (!executorService.isTerminated()) {
            LOGGER.log(Level.FINE, "Await async flush");
            executorService.shutdown();
            try {
                executorService.awaitTermination(5, TimeUnit.SECONDS);
                LOGGER.log(Level.FINE, "Async flush successfully completed");
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private void flush(int rowLength, int rowCounter, TableMetadata metadata,
                       BlockDto block, int totalLengthForHeader) throws IOException, ConnException {

        LOGGER.log(Level.FINE, MessageFormat.format("Flush data: rowLength=[{0}], rowCounter=[{1}], " +
                "metadata=[{2}], block=[{3}], totalLengthForHeader=[{4}]",
                rowLength, rowCounter, metadata, block, totalLengthForHeader));

        // Send put message
        messenger.put(rowCounter);

        // Send header with total binary insert
        ByteBuffer header_buffer = socket.generateHeaderedBuffer(totalLengthForHeader, false);
        socket.sendData(header_buffer, false);

        // Send available columns
        sendDataToSocket(rowLength, rowCounter, metadata, socket, block);
        messenger.isPutted();
    }

    private void sendDataToSocket(int rowLength, int rowCounter, TableMetadata tableMetadata, SQSocketConnector socket,
                                  BlockDto block) throws IOException, ConnException {
        for(int idx=0; idx < rowLength; idx++) {
            if(tableMetadata.isNullable(idx)) {
                socket.sendData((ByteBuffer) block.getNullBuffers()[idx].position(rowCounter), false);
            }
            if(tableMetadata.isTruVarchar(idx)) {
                socket.sendData(block.getNvarcLenBuffers()[idx], false);
            }
            socket.sendData(block.getDataBuffers()[idx], false);
        }
    }
}

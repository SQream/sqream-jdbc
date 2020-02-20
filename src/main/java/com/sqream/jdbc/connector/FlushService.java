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

    public BlockDto process(int rowCounter, TableMetadata metadata,
                        BlockDto block, int totalLengthForHeader, ByteBufferPool byteBufferPool, boolean async) {
        LOGGER.log(Level.FINE, MessageFormat.format(
                "Process block: rowCounter=[{1}], asynchronous=[{2}]", rowCounter, async));

        if (async) {
            if (executorService.isShutdown()) {
                executorService = Executors.newSingleThreadExecutor();
            }

            BlockDto blockForFlush = new BlockDto(block.getDataBuffers(), block.getNullBuffers(), block.getNvarcLenBuffers(), block.getCapacity());

            BlockDto blockFromPool = null;
            try {
                blockFromPool = byteBufferPool.getBlock();
            } catch (InterruptedException e) {
                throw new RuntimeException("Interrupted when getting block from byteBuffer pool", e);
            }

            executorService.submit(() -> {
                try {
                    Thread.currentThread().setName("flush-service");
                    flush(rowCounter,
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
                flush(rowCounter, metadata, block, totalLengthForHeader);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return block;
        }
    }

    public void awaitTermination() {
        LOGGER.log(Level.FINE, "Wait for termination");
        if (!executorService.isTerminated()) {
            LOGGER.log(Level.FINE, "Shutdown flush service");
            executorService.shutdown();
            try {
                if (!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
                    throw new RuntimeException("Could not send data to SQream server");
                }
                LOGGER.log(Level.FINE, "Async flush successfully completed");
            } catch (InterruptedException e) {
                executorService.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }

    private void flush(int rowCounter, TableMetadata metadata,
                       BlockDto block, int totalLengthForHeader) throws IOException, ConnException {

        LOGGER.log(Level.FINE, MessageFormat.format("Flush data: rowLength=[{0}], rowCounter=[{1}], " +
                "metadata=[{2}], block=[{3}], totalLengthForHeader=[{4}]",
                metadata.getRowLength(), rowCounter, metadata, block, totalLengthForHeader));

        // Send put message
        messenger.put(rowCounter);

        // Send header with total binary insert
        ByteBuffer header_buffer = socket.generateHeaderedBuffer(totalLengthForHeader, false);
        socket.sendData(header_buffer, false);

        // Send available columns
        sendDataToSocket(metadata.getRowLength(), rowCounter, metadata, socket, block);
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

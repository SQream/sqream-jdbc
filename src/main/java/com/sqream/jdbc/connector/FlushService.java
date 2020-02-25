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
        return new FlushService(socket, messenger);
    }

    public void process(TableMetadata metadata,
                        BlockDto block, int totalLengthForHeader, ByteBufferPool byteBufferPool) {
        LOGGER.log(Level.FINE, MessageFormat.format(
                "Process block: block=[{1}], asynchronous=[{2}]", block));

        executorService.submit(() -> {
            try {
                Thread.currentThread().setName("flush-service");
                flush(metadata, block, totalLengthForHeader);

                clearBuffers(block);

                byteBufferPool.releaseBlock(block);
            } catch (Exception e) {
                throw new RuntimeException("Exception when flush data", e);
            }
        });
    }

    private void clearBuffers(BlockDto block) {
        for (ByteBuffer dataBuffer : block.getDataBuffers()) {
            dataBuffer.clear();
        }
        for (ByteBuffer nullBuffer : block.getNullBuffers()) {
            if (nullBuffer != null) {
                nullBuffer.clear();
                //TODO: Alex K 13.01.2020 Check why previously allocated DirectByteBuffer and reset with HeapByteBuffer
                nullBuffer.put(ByteBuffer.allocate(block.getCapacity()));
                nullBuffer.clear();
            }
        }
        for (ByteBuffer nvarcLenBuffer : block.getNvarcLenBuffers()) {
            if (nvarcLenBuffer != null) {
                nvarcLenBuffer.clear();
            }
        }
    }


    public void close() {
        LOGGER.log(Level.FINE, "Close flush service");
        if (!executorService.isTerminated()) {
            LOGGER.log(Level.FINE, "Shutdown executor service");
            executorService.shutdown();
            try {
                if (!executorService.awaitTermination(1, TimeUnit.MINUTES)) {
                    throw new RuntimeException("Could not send data to SQream server");
                }
                LOGGER.log(Level.FINE, "Executor service terminated");
            } catch (InterruptedException e) {
                executorService.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }

    private void flush(TableMetadata metadata, BlockDto block, int totalLengthForHeader) throws IOException, ConnException {

        LOGGER.log(Level.FINE, MessageFormat.format(
                "Flush data: rowLength=[{0}], metadata=[{2}], block=[{3}], totalLengthForHeader=[{4}]",
                metadata.getRowLength(), metadata, block, totalLengthForHeader));

        // Send put message
        messenger.put(block.getFillSize());

        // Send header with total binary insert
        ByteBuffer header_buffer = socket.generateHeaderedBuffer(totalLengthForHeader, false);
        socket.sendData(header_buffer, false);

        // Send available columns
        sendDataToSocket(metadata.getRowLength(), block.getFillSize(), metadata, socket, block);
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

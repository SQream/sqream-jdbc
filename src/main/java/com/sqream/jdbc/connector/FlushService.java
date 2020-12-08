package com.sqream.jdbc.connector;

import com.sqream.jdbc.connector.serverAPI.Statement.SqreamExecutedStatement;

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

    private SqreamExecutedStatement sqreamExecutedStatement;
    private ExecutorService executorService = Executors.newSingleThreadExecutor();

    private FlushService(SqreamExecutedStatement sqreamExecutedStatement) {
        this.sqreamExecutedStatement = sqreamExecutedStatement;
    }

    public static FlushService getInstance(SqreamExecutedStatement sqreamExecutedStatement) {
        return new FlushService(sqreamExecutedStatement);
    }

    public void process(BlockDto block, ByteBufferPool byteBufferPool) {
        LOGGER.log(Level.FINE, MessageFormat.format("Process block: block=[{0}]", block));

        executorService.submit(() -> {
            try {
                Thread.currentThread().setName("flush-service");
                flush(sqreamExecutedStatement, block);

                resetBlock(block);

                byteBufferPool.releaseBlock(block);
            } catch (Exception e) {
                LOGGER.warning(e.getMessage());
                throw new RuntimeException("Exception when flush data", e);
            }
        });
    }

    private void resetBlock(BlockDto block) {
        clearBuffers(block);
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
                if (!executorService.awaitTermination(10, TimeUnit.MINUTES)) {
                    throw new RuntimeException("Could not stop executor service");
                }
                LOGGER.log(Level.FINE, "Executor service terminated");
            } catch (InterruptedException e) {
                executorService.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }

    private void flush(SqreamExecutedStatement sqreamExecutedStatement, BlockDto block) throws IOException, ConnException {

        LOGGER.log(Level.FINE, MessageFormat.format(
                "Flush data: block=[{0}]", block));

        sqreamExecutedStatement.put(block);
    }
}

package com.sqream.jdbc.connector;

import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.*;

public class ByteBufferCopierTest {

    private static final int POSITION = 2;
    private static final int LIMIT = 5;
    private static final int CAPACITY = 10;
    private static final int ARRAY_LENGTH = 10;

    private ByteBufferCopier copier;

    @Before
    public void setUp() {
        copier = new ByteBufferCopier();
    }

    @Test
    public void copyDirectByteBufferTest() {
        ByteBuffer original = prepareDirectByteBuffer();

        ByteBuffer copy = copier.copyByteBuffer(original);

        assertNotNull(copy);
        assertEquals(original.capacity(), copy.capacity());
        assertEquals(original.position(), copy.position());
        assertEquals(original.limit(), copy.limit());
    }

    @Test
    public void copyHeapByteBufferTest() {
        ByteBuffer original = prepareHeapByteBuffer();

        ByteBuffer copy = copier.copyByteBuffer(original);

        compareBuffers(original, copy);
    }

    @Test
    public void copyDirectByteBufferBlockTest() {
        BlockDto original = prepareDiferctByteBuffersBlock();

        long t0 = System.currentTimeMillis();
        BlockDto copy = copier.copyBlock(original);
        long t1 = System.currentTimeMillis();
        System.out.println("Copy direct byte buffers block: " + (t1 - t0));

        for (int i = 0; i < original.getDataBuffers().length; i++) {
            compareBuffers(original.getDataBuffers()[i], copy.getDataBuffers()[i]);
        }
        for (int i = 0; i < original.getNullBuffers().length; i++) {
            compareBuffers(original.getNullBuffers()[i], copy.getNullBuffers()[i]);
        }
        for (int i = 0; i < original.getNvarcLenBuffers().length; i++) {
            compareBuffers(original.getNvarcLenBuffers()[i], copy.getNvarcLenBuffers()[i]);
        }
    }

    @Test
    public void whenByteBufferIsNullReturnNullTest() {
        ByteBuffer copy = copier.copyByteBuffer(null);

        assertNull(copy);
    }

    @Test
    public void whenBlockIsNullReturnNullTest() {
        BlockDto copy = copier.copyBlock(null);

        assertNull(copy);
    }

    @Test
    public void copyHeapByteBufferBlockTest() {
        BlockDto original = prepareHeapByteBuffersBlock();

        long t0 = System.currentTimeMillis();
        BlockDto copy = copier.copyBlock(original);
        long t1 = System.currentTimeMillis();
        System.out.println("Copy heap byte buffers block: " + (t1 - t0));

        for (int i = 0; i < original.getDataBuffers().length; i++) {
            compareBuffers(original.getDataBuffers()[i], copy.getDataBuffers()[i]);
        }
        for (int i = 0; i < original.getNullBuffers().length; i++) {
            compareBuffers(original.getNullBuffers()[i], copy.getNullBuffers()[i]);
        }
        for (int i = 0; i < original.getNvarcLenBuffers().length; i++) {
            compareBuffers(original.getNvarcLenBuffers()[i], copy.getNvarcLenBuffers()[i]);
        }
    }

    private ByteBuffer prepareDirectByteBuffer() {
        ByteBuffer result = ByteBuffer.allocateDirect(CAPACITY);
        result.position(POSITION);
        result.limit(LIMIT);
        return result;
    }

    private ByteBuffer prepareHeapByteBuffer() {
        ByteBuffer result = ByteBuffer.allocate(CAPACITY);
        result.position(POSITION);
        result.limit(LIMIT);
        return result;
    }

    private BlockDto prepareDiferctByteBuffersBlock() {
        ByteBuffer[] dataBuffers = prepareDirectByteBufferArray(ARRAY_LENGTH);
        ByteBuffer[] nullBuffers = prepareDirectByteBufferArray(ARRAY_LENGTH);
        ByteBuffer[] nvarcLenBuffers = prepareDirectByteBufferArray(ARRAY_LENGTH);
        return new BlockDto(dataBuffers, nullBuffers, nvarcLenBuffers, CAPACITY);
    }

    private BlockDto prepareHeapByteBuffersBlock() {
        ByteBuffer[] dataBuffers = prepareHeapByteBufferArray(ARRAY_LENGTH);
        ByteBuffer[] nullBuffers = prepareHeapByteBufferArray(ARRAY_LENGTH);
        ByteBuffer[] nvarcLenBuffers = prepareHeapByteBufferArray(ARRAY_LENGTH);
        return new BlockDto(dataBuffers, nullBuffers, nvarcLenBuffers, CAPACITY);
    }

    private ByteBuffer[] prepareDirectByteBufferArray(int length) {
        ByteBuffer[] result = new ByteBuffer[length];
        for (int i = 0; i < length; i++) {
            result[i] = prepareDirectByteBuffer();
        }
        return result;
    }

    private ByteBuffer[] prepareHeapByteBufferArray(int length) {
        ByteBuffer[] result = new ByteBuffer[length];
        for (int i = 0; i < length; i++) {
            result[i] = prepareHeapByteBuffer();
        }
        return result;
    }

    private void compareBuffers(ByteBuffer original, ByteBuffer copy) {
        assertNotNull(copy);
        assertEquals(original.capacity(), copy.capacity());
        assertEquals(original.position(), copy.position());
        assertEquals(original.limit(), copy.limit());
    }
}
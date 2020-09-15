package com.sqream.jdbc.connector.byteWriters;

import java.nio.ByteBuffer;

public abstract class BaseWriter implements ByteWriter {

    @Override
    public int writeInt(ByteBuffer buffer, Integer value) {
        throw new UnsupportedOperationException(
                String.format("Trying to set a value of type [Integer] to column type [%s]", getColumnType()));
    }

    @Override
    public int writeShort(ByteBuffer buffer, Short value) {
        throw new UnsupportedOperationException(
                String.format("Trying to set a value of type [Short] to column type [%s]", getColumnType()));
    }

    @Override
    public int writeLong(ByteBuffer buffer, Long value) {
        throw new UnsupportedOperationException(
                String.format("Trying to set a value of type [Long] to column type [%s]", getColumnType()));
    }

    @Override
    public int writeFloat(ByteBuffer buffer, Float value) {
        throw new UnsupportedOperationException(
                String.format("Trying to set a value of type [Float] to column type [%s]", getColumnType()));
    }

    @Override
    public int writeDouble(ByteBuffer buffer, Double value) {
        throw new UnsupportedOperationException(
                String.format("Trying to set a value of type [Double] to column type [%s]", getColumnType()));
    }

    @Override
    public int writeBoolean(ByteBuffer buffer, byte value) {
        throw new UnsupportedOperationException(
                String.format("Trying to set a value of type [Boolean] to column type [%s]", getColumnType()));
    }

    @Override
    public int writeUbyte(ByteBuffer buffer, Byte value) {
        throw new UnsupportedOperationException(
                String.format("Trying to set a value of type [Ubyte] to column type [%s]", getColumnType()));
    }

    @Override
    public int writeDate(ByteBuffer buffer, int value) {
        throw new UnsupportedOperationException(
                String.format("Trying to set a value of type [Date] to column type [%s]", getColumnType()));
    }

    @Override
    public int writeDateTime(ByteBuffer buffer, long value) {
        throw new UnsupportedOperationException(
                String.format("Trying to set a value of type [DateTime] to column type [%s]", getColumnType()));
    }

    @Override
    public int writeVarchar(ByteBuffer buffer, byte[] value, int columnLength) {
        throw new UnsupportedOperationException(
                String.format("Trying to set a value of type [Varchar] to column type [%s]", getColumnType()));
    }

    @Override
    public int writeNvarchar(ByteBuffer buffer, byte[] value) {
        throw new UnsupportedOperationException(
                String.format("Trying to set a value of type [Nvarchar] to column type [%s]", getColumnType()));
    }

    abstract String getColumnType();
}

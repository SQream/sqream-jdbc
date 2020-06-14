package com.sqream.jdbc.connector.byteWriters;

import java.nio.ByteBuffer;

public abstract class BaseWriter implements ByteWriter {

    @Override
    public void writeInt(ByteBuffer buffer, Integer value) {
        throw new UnsupportedOperationException(
                String.format("Trying to set a value of type [Integer] to column type [%s]", getColumnType()));
    }

    @Override
    public void writeShort(ByteBuffer buffer, Short value) {
        throw new UnsupportedOperationException(
                String.format("Trying to set a value of type [Short] to column type [%s]", getColumnType()));
    }

    @Override
    public void writeLong(ByteBuffer buffer, Long value) {
        throw new UnsupportedOperationException(
                String.format("Trying to set a value of type [Long] to column type [%s]", getColumnType()));
    }

    @Override
    public void writeFloat(ByteBuffer buffer, Float value) {
        throw new UnsupportedOperationException(
                String.format("Trying to set a value of type [Float] to column type [%s]", getColumnType()));
    }

    @Override
    public void writeDouble(ByteBuffer buffer, Double value) {
        throw new UnsupportedOperationException(
                String.format("Trying to set a value of type [Double] to column type [%s]", getColumnType()));
    }

    @Override
    public void writeBoolean(ByteBuffer buffer, byte value) {
        throw new UnsupportedOperationException(
                String.format("Trying to set a value of type [Boolean] to column type [%s]", getColumnType()));
    }

    @Override
    public void writeUbyte(ByteBuffer buffer, Byte value) {
        throw new UnsupportedOperationException(
                String.format("Trying to set a value of type [Ubyte] to column type [%s]", getColumnType()));
    }

    @Override
    public void writeDate(ByteBuffer buffer, int value) {
        throw new UnsupportedOperationException(
                String.format("Trying to set a value of type [Date] to column type [%s]", getColumnType()));
    }

    @Override
    public void writeDateTime(ByteBuffer buffer, long value) {
        throw new UnsupportedOperationException(
                String.format("Trying to set a value of type [DateTime] to column type [%s]", getColumnType()));
    }

    @Override
    public void writeVarchar(ByteBuffer buffer, byte[] value, int columnLength) {
        throw new UnsupportedOperationException(
                String.format("Trying to set a value of type [Varchar] to column type [%s]", getColumnType()));
    }

    @Override
    public void writeNvarchar(ByteBuffer buffer, byte[] value) {
        throw new UnsupportedOperationException(
                String.format("Trying to set a value of type [Nvarchar] to column type [%s]", getColumnType()));
    }

    abstract String getColumnType();
}

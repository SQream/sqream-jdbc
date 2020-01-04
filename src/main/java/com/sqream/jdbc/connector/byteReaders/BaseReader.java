package com.sqream.jdbc.connector.byteReaders;

import java.nio.ByteBuffer;

public abstract class BaseReader implements ByteReader {
    @Override
    public Integer readInt(ByteBuffer buffer, int rowIndex) {
        throw new UnsupportedOperationException(
                String.format("Trying to get a value of type [Integer] from column type [%s]", getColumnType()));
    }

    @Override
    public Short readShort(ByteBuffer buffer, int rowIndex) {
        throw new UnsupportedOperationException(
                String.format("Trying to get a value of type [Short] from column type [%s]", getColumnType()));
    }

    @Override
    public Long readLong(ByteBuffer buffer, int rowIndex) {
        throw new UnsupportedOperationException(
                String.format("Trying to get a value of type [Long] from column type [%s]", getColumnType()));
    }

    @Override
    public Float readFloat(ByteBuffer buffer, int rowIndex) {
        throw new UnsupportedOperationException(
                String.format("Trying to get a value of type [Float] from column type [%s]", getColumnType()));
    }

    @Override
    public Double readDouble(ByteBuffer buffer, int rowIndex) {
        throw new UnsupportedOperationException(
                String.format("Trying to get a value of type [Double] from column type [%s]", getColumnType()));
    }

    @Override
    public Boolean readBoolean(ByteBuffer buffer, int rowIndex) {
        throw new UnsupportedOperationException(
                String.format("Trying to get a value of type [Boolean] from column type [%s]", getColumnType()));
    }

    @Override
    public Byte readUbyte(ByteBuffer buffer, int rowIndex) {
        throw new UnsupportedOperationException(
                String.format("Trying to get a value of type [Ubyte] from column type [%s]", getColumnType()));
    }

    @Override
    public int readDate(ByteBuffer buffer, int rowIndex) {
        throw new UnsupportedOperationException(
                String.format("Trying to get a value of type [Date] from column type [%s]", getColumnType()));
    }

    @Override
    public long readDateTime(ByteBuffer buffer, int rowIndex) {
        throw new UnsupportedOperationException("Trying to get a value of type [DateTime] from column type [ftBool]");
    }

    abstract String getColumnType();
}

package com.sqream.jdbc.connector.storage;

import com.sqream.jdbc.connector.BlockDto;
import com.sqream.jdbc.connector.ConnException;

import java.nio.charset.Charset;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.ZoneId;

public interface Storage {

    boolean next() throws ConnException;

    void setBlock(BlockDto block);

    void clearBuffers(int row_length);

    BlockDto getBlock();

    void setBoolean(int colIndex, Boolean value);

    void setUbyte(int colIndex, Byte value);

    void setShort(int colIndex, Short value);

    void setInt(int colIndex, Integer value);

    void setLong(int colIndex, Long value);

    void setFloat(int colIndex, Float value);

    void setDouble(int colIndex, Double value);

    void setVarchar(int colIndex, byte[] stringBytes, String originalString);

    void setNvarchar(int colIndex, byte[] stringBytes, String originalString);

    void setDate(int colIndex, Date date, ZoneId zone);

    void setDatetime(int colIndex, Timestamp timestamp, ZoneId zone);

    Boolean getBoolean(int colIndex);

    Byte getUbyte(int colIndex);

    Short getShort(int colIndex);

    Integer getInt(int colIndex);

    Long getLong(int colIndex);

    Float getFloat(int colIndex);

    Double getDouble(int colIndex);

    Date getDate(int colIndex, ZoneId zoneId);

    Timestamp getTimestamp(int colIndex, ZoneId zoneId);

    String getVarchar(int colIndex, String varcharEncoding);

    String getNvarchar(int colIndex, Charset varcharEncoding);

    int getTotalLengthForHeader(int row_length, int row_counter);
}

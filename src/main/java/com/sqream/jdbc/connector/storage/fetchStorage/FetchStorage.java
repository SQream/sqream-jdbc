package com.sqream.jdbc.connector.storage.fetchStorage;

import com.sqream.jdbc.connector.BlockDto;

import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.ZoneId;

public interface FetchStorage {

    boolean next();

    void setBlock(BlockDto block);

    Boolean getBoolean(int colIndex);

    Byte getUbyte(int colIndex);

    Short getShort(int colIndex);

    Integer getInt(int colIndex);

    Long getLong(int colIndex);

    Float getFloat(int colIndex);

    Double getDouble(int colIndex);

    BigDecimal getBigDecimal(int colIndex);

    Date getDate(int colIndex, ZoneId zoneId);

    Timestamp getTimestamp(int colIndex, ZoneId zoneId);

    String getVarchar(int colIndex, String varcharEncoding);

    String getNvarchar(int colIndex, Charset varcharEncoding);
}

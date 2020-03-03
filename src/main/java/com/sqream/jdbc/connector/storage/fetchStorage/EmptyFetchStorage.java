package com.sqream.jdbc.connector.storage.fetchStorage;

import com.sqream.jdbc.connector.BlockDto;

import java.nio.charset.Charset;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.ZoneId;

public class EmptyFetchStorage implements FetchStorage {

    @Override
    public boolean next() {
        return false;
    }

    @Override
    public void setBlock(BlockDto block) {

    }

    @Override
    public Boolean getBoolean(int colIndex) {
        return null;
    }

    @Override
    public Byte getUbyte(int colIndex) {
        return null;
    }

    @Override
    public Short getShort(int colIndex) {
        return null;
    }

    @Override
    public Integer getInt(int colIndex) {
        return null;
    }

    @Override
    public Long getLong(int colIndex) {
        return null;
    }

    @Override
    public Float getFloat(int colIndex) {
        return null;
    }

    @Override
    public Double getDouble(int colIndex) {
        return null;
    }

    @Override
    public Date getDate(int colIndex, ZoneId zoneId) {
        return null;
    }

    @Override
    public Timestamp getTimestamp(int colIndex, ZoneId zoneId) {
        return null;
    }

    @Override
    public String getVarchar(int colIndex, String varcharEncoding) {
        return null;
    }

    @Override
    public String getNvarchar(int colIndex, Charset varcharEncoding) {
        return null;
    }
}

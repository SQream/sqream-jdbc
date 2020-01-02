package com.sqream.jdbc.connector.byteReaders;

import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Map;

public class ByteReaderFactory {

    private static final Map<String, ByteReader> readerMap = new HashMap<>();

    static {
        readerMap.put("ftShort", new FtShortReader());
        readerMap.put("ftUByte", new FtUbyteReader());
        readerMap.put("ftInt", new FtIntReader());
        readerMap.put("ftLong", new FtLongReader());
        readerMap.put("ftFloat", new FtFloatReader());
        readerMap.put("ftDouble", new FtDoubleReader());
        readerMap.put("ftBool", new FtBooleanReader());
    }

    public static ByteReader getReader(String columnType) {
        ByteReader result = readerMap.get(columnType);
        if (result == null) {
            throw new IllegalArgumentException(MessageFormat.format("Can not read from column type [{0}]", columnType));
        }
        return result;
    }
}

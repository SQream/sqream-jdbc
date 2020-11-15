package com.sqream.jdbc.connector.byteWriters;

import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Map;

public class ByteWriterFactory {

    private static final Map<String, ByteWriter> writerMap = new HashMap<>();

    static {
        writerMap.put("ftShort", new FtShortWriter());
        writerMap.put("ftUByte", new FtUbyteWriter());
        writerMap.put("ftInt", new FtIntWriter());
        writerMap.put("ftLong", new FtLongWriter());
        writerMap.put("ftFloat", new FtFloatWriter());
        writerMap.put("ftDouble", new FtDoubleWriter());
        writerMap.put("ftBool", new FtBooleanWriter());
        writerMap.put("ftDate", new FtDateWriter());
        writerMap.put("ftDateTime", new FtDateTimeWriter());
        writerMap.put("ftVarchar", new FtVarcharWriter());
        writerMap.put("ftBlob", new FtNvarcharWriter());
        writerMap.put("ftNumeric", new FtNumericWriter());
    }

    public static ByteWriter getWriter(String columnType) {
        ByteWriter result = writerMap.get(columnType);
        if (result == null) {
            throw new IllegalArgumentException(
                    MessageFormat.format("Can not write to column type [{0}]", columnType));
        }
        return result;
    }
}

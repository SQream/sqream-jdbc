package com.sqream.jdbc.connector.byteReaders;

import com.sqream.jdbc.connector.ColumnTypes;
import org.junit.Test;

import java.text.MessageFormat;

public class ByteReaderFactoryTest {

    @Test
    public void factoryHasImplementationForTypeTest() {
        String checkedType = "";
        try {
            for (ColumnTypes type : ColumnTypes.values()) {
                checkedType = type.getValue();
                ByteReaderFactory.getReader(checkedType);
            }
        } catch (Exception e) {
            throw new RuntimeException(MessageFormat.format(
                    "ByteReaderFactory does not have implementation for [{0}] column type", checkedType), e);
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void getReaderForIncorrectColumnTypeTest() {
        ByteReaderFactory.getReader("SomeUnsupportedColumnType");
    }
}
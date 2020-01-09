package com.sqream.jdbc.connector;

import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class InsertValidatorTest {

    private TableMetadata metadata;
    private InsertValidator validator;

    @Before
    public void setUp() {
        this.metadata = mock(TableMetadata.class);
        this.validator = new InsertValidator(metadata);
    }

    @Test(expected = IllegalArgumentException.class)
    public void whenColumnIndexLessThanZeroTest() {
        int rowLength = 10;
        when(metadata.getRowLength()).thenReturn(rowLength);
        validator.validateColumnIndex(-1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void whenColumnIndexEqualsRowLengthTest() {
        int rowLength = 10;
        int wrongIndex = rowLength;
        when(metadata.getRowLength()).thenReturn(rowLength);

        validator.validateColumnIndex(wrongIndex);
    }

    @Test(expected = IllegalArgumentException.class)
    public void whenColumnIndexMoreThanRowLengthTest() {
        int rowLength = 10;
        int wrongIndex = rowLength + 1;
        when(metadata.getRowLength()).thenReturn(rowLength);

        validator.validateColumnIndex(wrongIndex);
    }

    @Test
    public void validateColumnIndexTest() {
        int rowLength = 10;
        int correctIndex = rowLength - 1;

        when(metadata.getRowLength()).thenReturn(rowLength);

        validator.validateColumnIndex(correctIndex);
    }

    @Test
    public void validateGetTypeTest() {
        int columnIndex = 5;
        String columnType = "correctType";
        when(metadata.getType(columnIndex)).thenReturn(columnType);

        validator.validateGetType(columnIndex, columnType);
    }

    @Test(expected = IllegalArgumentException.class)
    public void whenInvalidColumnTypeTest() {
        int columnIndex = 5;
        String columnType = "correctType";
        String anotherType = "anotherType";
        when(metadata.getType(columnIndex)).thenReturn(anotherType);

        validator.validateGetType(columnIndex, columnType);
    }

    @Test(expected = IllegalArgumentException.class)
    public void whenColumnTypeIsNullTest() {
        int columnIndex = 5;
        String columnType = "correctType";
        when(metadata.getType(columnIndex)).thenReturn(columnType);

        validator.validateGetType(columnIndex, null);
    }

    @Test
    public void whenUByteIsNullTest() {
        validator.validateUbyte(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void whenUByteIsNegativeTest() {
        validator.validateUbyte((byte) -1);
    }

    @Test
    public void validateUbyteTest() {
        validator.validateUbyte((byte) 0);
        validator.validateUbyte((byte) 1);
    }

    @Test
    public void validateVarcharTest() {
        int columnSize = 20;
        int stringSize = 20;
        int columnIndex = 1;
        when(metadata.getSize(columnIndex)).thenReturn(columnSize);

        validator.validateVarchar(columnIndex, stringSize);

    }

    @Test(expected = IllegalArgumentException.class)
    public void whenStringSizeMoreThanColumnSizeTest() {
        int columnSize = 20;
        int stringSize = 21;
        int columnIndex = 1;
        when(metadata.getSize(columnIndex)).thenReturn(columnSize);

        validator.validateVarchar(columnIndex, stringSize);
    }

    @Test
    public void whenSetNullInNullableTest() {
        int columnIndex = 1;
        String columnType = "ColumnType";
        when(metadata.isNullable(columnIndex)).thenReturn(true);
        when(metadata.getRowLength()).thenReturn(3);
        when(metadata.getType(columnIndex)).thenReturn(columnType);

        validator.validateSet(columnIndex, null, columnType);
    }

    @Test(expected = IllegalArgumentException.class)
    public void whenSetNullInNotNullableTest() {
        Object value = null;
        int columnIndex = 1;
        when(metadata.isNullable(columnIndex)).thenReturn(false);
        when(metadata.getRowLength()).thenReturn(3);

        try {
            validator.validateSet(columnIndex, value, "columnType");
        } catch (IllegalArgumentException e) {
            if (e.getMessage().contains("Trying to set null on non nullable column")) {
                throw new IllegalArgumentException(e);
            } else {
                throw new RuntimeException("Not correct exception message");
            }
        }
    }

    @Test
    public void validateSetTest() {
        int columnIndex = 1;
        Object value = 10;
        String columnType = "columnType";
        when(metadata.isNullable(columnIndex)).thenReturn(false);
        when(metadata.getRowLength()).thenReturn(3);
        when(metadata.getType(columnIndex)).thenReturn(columnType);

        validator.validateSet(columnIndex, value, columnType);
    }

    @Test(expected = IllegalArgumentException.class)
    public void whenWrongTypeTest() {
        int columnIndex = 1;
        Object value = 10;
        String columnType = "columnType";
        String anotherColumnType = "wrongColumnType";
        when(metadata.isNullable(columnIndex)).thenReturn(false);
        when(metadata.getRowLength()).thenReturn(3);
        when(metadata.getType(columnIndex)).thenReturn(columnType);

        try {
            validator.validateSet(columnIndex, value, anotherColumnType);
        } catch (IllegalArgumentException e) {
            if (e.getMessage().contains("Trying to set")) {
                throw e;
            } else {
                throw new RuntimeException("Wrong exception message");
            }
        }
    }
}
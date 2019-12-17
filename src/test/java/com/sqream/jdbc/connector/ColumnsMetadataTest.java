package com.sqream.jdbc.connector;

import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

public class ColumnsMetadataTest {

    private ColumnsMetadata metaData;
    private static final int ROW_LENGTH = 5;
    private List<String> EXPECTED_ALL_NAMES;
    private List<String> EXPECTED_ALL_TYPES;
    private List<Integer> EXPECTED_ALL_SIZES;
    private List<Boolean> EXPECTED_NULLABLES;
    private List<Boolean> EXPECTED_TRU_VARCHAR;
    private int EXPECTED_SIZE_SUM;
    private int EXPECTED_MAX_SIZE;

    @Before
    public void setUp() {
        metaData = new ColumnsMetadata();
        EXPECTED_ALL_NAMES = new ArrayList<>(ROW_LENGTH);
        EXPECTED_ALL_TYPES = new ArrayList<>(ROW_LENGTH);
        EXPECTED_ALL_SIZES = new ArrayList<>(ROW_LENGTH);
        EXPECTED_NULLABLES = new ArrayList<>(ROW_LENGTH);
        EXPECTED_TRU_VARCHAR = new ArrayList<>(ROW_LENGTH);
        EXPECTED_SIZE_SUM = 0;

        metaData.init(ROW_LENGTH);
        for (int i = 0; i < ROW_LENGTH; i++) {
            String testName = String.format("COL_%s_TEST_NAME", i + 1);
            String testType = String.format("COL_%s_TEST_TYPE", i + 1);
            int curSize = 10 + i;
            boolean isNullable = i % 2 == 0;
            boolean isTruVarchar = i % 2 == 0;
            metaData.setByIndex(i, new ColumnMetadataDto(isTruVarchar, testName, isNullable, testType, curSize));

            EXPECTED_ALL_NAMES.add(testName);
            EXPECTED_ALL_TYPES.add(testType);
            EXPECTED_ALL_SIZES.add(curSize);
            EXPECTED_NULLABLES.add(isNullable);
            EXPECTED_TRU_VARCHAR.add(isTruVarchar);
            EXPECTED_SIZE_SUM += curSize;
            saveMaxSize(curSize);
        }
    }

    private void saveMaxSize(int size) {
        if (EXPECTED_MAX_SIZE < size) {
            EXPECTED_MAX_SIZE = size;
        }
    }

    @Test
    public void commonGettersTest() {
        assertNotNull(metaData);
        // check that metadata keep names in lowercase.
        assertFalse(metaData.getAllNames().containsAll(EXPECTED_ALL_NAMES));
        assertTrue(metaData.getAllNames().containsAll(EXPECTED_ALL_NAMES
                .stream()
                .map(String::toLowerCase)
                .collect(Collectors.toList())));
        assertEquals(EXPECTED_MAX_SIZE, metaData.getMaxSize());
        assertEquals(EXPECTED_SIZE_SUM, metaData.getSizesSum());
    }

    @Test
    public void getColNumByNameTest() {
        assertTrue(EXPECTED_ALL_NAMES.size() > 0);
        for (int i = 0; i < EXPECTED_ALL_NAMES.size(); i++) {
            // check that getIndexByName() case sensitive
            assertNull(metaData.getColNumByName(EXPECTED_ALL_NAMES.get(i)));
            assertEquals(Integer.valueOf(i + 1), metaData.getColNumByName(EXPECTED_ALL_NAMES.get(i).toLowerCase()));
        }
    }

    @Test
    public void getTypeTest() {
        assertTrue(EXPECTED_ALL_TYPES.size() > 0);
        for (int i = 0; i < EXPECTED_ALL_TYPES.size(); i++) {
            assertEquals(EXPECTED_ALL_TYPES.get(i), metaData.getType(i));
        }
    }

    @Test
    public void getSizeTest() {
        assertTrue(EXPECTED_ALL_SIZES.size() > 0);
        for (int i = 0; i < EXPECTED_ALL_SIZES.size(); i++) {
            assertEquals(EXPECTED_ALL_SIZES.get(i), Integer.valueOf(metaData.getSize(i)));
        }
    }

    @Test
    public void isNullableTest() {
        assertTrue(EXPECTED_NULLABLES.size() > 0);
        for (int i = 0; i < EXPECTED_NULLABLES.size(); i++) {
            assertEquals(EXPECTED_NULLABLES.get(i), metaData.isNullable(i));
        }
    }

    @Test
    public void getNameTest() {
        assertTrue(EXPECTED_ALL_NAMES.size() > 0);
        for (int i = 0; i < EXPECTED_ALL_NAMES.size(); i++) {
            assertEquals(EXPECTED_ALL_NAMES.get(i), metaData.getName(i));
        }
    }

    @Test
    public void isTruVarcharTest() {
        assertTrue(EXPECTED_TRU_VARCHAR.size() > 0);
        for (int i = 0; i < EXPECTED_TRU_VARCHAR.size(); i++) {
            assertEquals(EXPECTED_TRU_VARCHAR.get(i), metaData.isTruVarchar(i));
        }
    }

    @Test
    public void getAmountNullablleColumnsTest() {
        assertTrue(EXPECTED_NULLABLES.size() > 0);
        List<Boolean> nullableColumns = EXPECTED_NULLABLES.stream().filter(col -> col).collect(Collectors.toList());
        assertEquals(nullableColumns.size(), metaData.getAmountNullablleColumns());
    }
}
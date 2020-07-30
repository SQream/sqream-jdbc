package com.sqream.jdbc;

import de.vandermeer.asciitable.AsciiTable;
import org.junit.Assert;
import org.junit.Test;

import java.sql.*;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;

import static com.sqream.jdbc.Perf.ColType.*;
import static com.sqream.jdbc.TestEnvironment.createConnection;

public class Perf {
    private static final int[] columnCounts = new int[]{1, 10, 100};
    private static final int[] varcharSizes = new int[]{10, 100, 400};
    private static final int[] rowCounts = new int[]{1, 1000, 10000, 100000, 1000000};
    private static final int selectAllColAmount = 200;
    private static final int varcharSizeForSelectAll = 10;
    private static final int limitForSelectAll = 100000;
    private static final Map<ColType, BiConsumer<ResultSet, Integer>> gettersMap = new HashMap<>();
    private static final Map<ColType, BiConsumer<PreparedStatement, Integer>> settersMap = new HashMap<>();
    private static int index = 0;
    private static AsciiTable resultTable;

    private static final Date testDate = new Date(System.currentTimeMillis());
    private static final Timestamp testDateTime = new Timestamp(System.currentTimeMillis());
    private static String testText = "";

    enum ColType {
        BOOL("bool", Byte.BYTES),
        TINYINT("tinyint", Byte.BYTES),
        SMALLINT("smallint", Short.BYTES),
        INT("int", Integer.BYTES),
        BIGINT("bigint", Long.BYTES),
        REAL("real", Float.BYTES),
        DOUBLE("double", Double.BYTES),
        DATE("date", Integer.BYTES),
        DATETIME("datetime", Long.BYTES),
        VARCHAR("varchar", Byte.BYTES),
        NVARCHAR("nvarchar", Byte.BYTES);

        private final String value;
        private final int size;

        ColType(String value, int size) {
            this.value = value;
            this.size = size;
        }

        public String getValue() {
            return this.value;
        }

        public int getSize() {
            return this.size;
        }
    }

    {
        gettersMap.put(BOOL, this::getBool);
        gettersMap.put(TINYINT, this::getTinyInt);
        gettersMap.put(SMALLINT, this::getSmallInt);
        gettersMap.put(INT, this::getInt);
        gettersMap.put(BIGINT, this::getBigInt);
        gettersMap.put(REAL, this::getReal);
        gettersMap.put(DOUBLE, this::getDouble);
        gettersMap.put(DATE, this::getDate);
        gettersMap.put(DATETIME, this::getDatetime);
        gettersMap.put(VARCHAR, this::getText);
        gettersMap.put(NVARCHAR, this::getText);

        settersMap.put(BOOL, this::setBool);
        settersMap.put(TINYINT, this::setTinyInt);
        settersMap.put(SMALLINT, this::setSmallInt);
        settersMap.put(INT, this::setInt);
        settersMap.put(BIGINT, this::setBigInt);
        settersMap.put(REAL, this::setReal);
        settersMap.put(DOUBLE, this::setDouble);
        settersMap.put(DATE, this::setDate);
        settersMap.put(DATETIME, this::setDatetime);
        settersMap.put(VARCHAR, this::setText);
        settersMap.put(NVARCHAR, this::setText);
    }

    @Test
    public void selectTest() {
        resultTable = new AsciiTable();
        resultTable.addRule();
        resultTable.addRow("index", "field", "row length", "columns", "rows", "total ms", "per 1Mb");
        resultTable.addRule();
        Arrays.stream(values()).forEach(this::select);
        selectAll();
        resultTable.addRule();
        System.out.println(resultTable.render());
    }

    @Test
    public void insertTest() {
        resultTable = new AsciiTable();
        resultTable.addRule();
        resultTable.addRow("index", "field", "row length", "columns", "rows", "total ms", "per 1Mb");
        resultTable.addRule();
        Arrays.stream(values()).forEach(this::insert);
        resultTable.addRule();
        System.out.println(resultTable.render());
    }

    private void select(ColType type) {
        BiConsumer<ResultSet, Integer> getter = gettersMap.get(type);
        try (Connection conn = createConnection(); Statement stmt = conn.createStatement()) {
            for (int colAmount : columnCounts) {
                for (int rowAmount : rowCounts) {
                    for (int textLength : varcharSizes) {
                        if (isTextType(type) || textLength == varcharSizes[0]) {
                            long startTime = System.currentTimeMillis();
                            stmt.setFetchSize(1);
                            ResultSet rs = stmt.executeQuery(generateSelectQuery(type, colAmount, rowAmount, textLength));
                            int rowCounter = 0;
                            while (rs.next()) {
                                for (int i = 0; i < colAmount; i++) {
                                    getter.accept(rs, i);
                                }
                                rowCounter++;
                            }
                            long totalTime = System.currentTimeMillis() - startTime;
                            long rowLength = rowLength(type, colAmount, textLength);
                            resultTable.addRow(index, type, rowLength, colAmount, rowAmount, totalTime, (1024 * 1024 * totalTime) / (rowLength * rowAmount));
                            Assert.assertEquals(rowAmount, rowCounter);
                            index++;
                        }
                    }
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void selectAll() {
        int colAmountPerType = selectAllColAmount / ColType.values().length;
        try (Connection conn = createConnection();
             Statement stmt = conn.createStatement()) {

            long startTime = System.currentTimeMillis();
            ResultSet rs = stmt.executeQuery(generateSelectAllQuery(colAmountPerType));
            BiConsumer<ResultSet, Integer> getter;
            int rowCounter = 0;
            int colIndex;
            while (rs.next()) {
                colIndex = 0;
                for (ColType type : ColType.values()) {
                    getter = gettersMap.get(type);
                    for (int i = 0; i < colAmountPerType; i++) {
                        getter.accept(rs, colIndex);
                        colIndex++;
                    }
                }
                rowCounter++;
            }
            Assert.assertEquals(limitForSelectAll, rowCounter);
            long totalTime = System.currentTimeMillis() - startTime;
            long rowLength = rowLengthAll(colAmountPerType);
            resultTable.addRow(index, "ALL", rowLength, ColType.values().length * colAmountPerType, limitForSelectAll, totalTime, (1024 * 1024 * totalTime) / (rowLength * limitForSelectAll));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void insert(ColType type) {
        BiConsumer<PreparedStatement, Integer> setter = settersMap.get(type);
        try (Connection conn = createConnection()) {
            for (int colAmount : columnCounts) {
                for (int rowAmount : rowCounts) {
//                    for (int textLength : varcharSizes) {
                        if (!isTextType(type) || colAmount <= 10) {
                            testText = String.join("", Collections.nCopies(varcharSizes[0], "a"));
                            int rowCounter = 0;
                            long startTime = System.currentTimeMillis();
                            try (PreparedStatement pstmt = conn.prepareStatement(generateInsertQuery(type, colAmount, varcharSizes[0]))) {
                                for (int rowIndex = 0; rowIndex < rowAmount; rowIndex++) {
                                    for (int colIndex = 0; colIndex < colAmount; colIndex++) {
                                        setter.accept(pstmt, colIndex);
                                    }
                                    pstmt.addBatch();
                                    if (((rowIndex + 1) % 100_000 == 0 && rowIndex > 1) || rowIndex == rowAmount - 1) {
                                        pstmt.executeBatch();
                                    }
                                    rowCounter++;
                                }
                            }
                            long totalTime = System.currentTimeMillis() - startTime;
                            long rowLength = rowLength(type, colAmount, varcharSizes[0]);
                            resultTable.addRow(index, type, rowLength, colAmount, rowAmount, totalTime, (1024 * 1024 * totalTime) / (rowLength * rowAmount));
                            Assert.assertEquals(rowAmount, rowCounter);
                            index++;
//                        }
                    }
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private String generateSelectQuery(ColType type, int colAmount, int rowAmount, int textLength) {
        StringBuilder sb = new StringBuilder();
        sb.append("select ");
        for (int i = 0; i < colAmount; i++) {
            sb.append(generateColDefinition(type, textLength, i));
            if (i < colAmount - 1) {
                sb.append(", ");
            }
        }
        sb.append(" from random limit ");
        sb.append(rowAmount);
        sb.append(";");
        return sb.toString();
    }

    private String generateInsertQuery(ColType type, int colAmount, int textLength) {
        StringBuilder sb = new StringBuilder();
        sb.append("insert into random values(");
        for (int i = 0; i < colAmount; i++) {
            sb.append(generateColDefinition(type, textLength, i));
            if (i < colAmount - 1) {
                sb.append(", ");
            }
        }
        sb.append(");");
        return sb.toString();
    }

    private String generateColDefinition(ColType colType, int textLength, int colIndex) {
        StringBuilder sb = new StringBuilder();
        sb.append(colType.getValue());
        sb.append("?name=col");
        sb.append(colIndex + 1);
        if (isTextType(colType)) {
            sb.append("&length=");
            sb.append(textLength);
        }
        return sb.toString();
    }

    private String generateSelectAllQuery(int colAmountPerType) {
        StringBuilder sb = new StringBuilder();
        sb.append("select ");
        for (ColType type : ColType.values()) {
            for (int i = 0; i < colAmountPerType; i++) {
                sb.append(generateColDefinition(type, varcharSizeForSelectAll, i));
                if (i < colAmountPerType - 1) {
                    sb.append(", ");
                }
            }
            sb.append(", ");
        }
        sb.append(" from random limit ");
        sb.append(limitForSelectAll);
        sb.append(";");
        return sb.toString();
    }

    private void getBool(ResultSet rs, int colIndex) {
        try {
            rs.getBoolean(colIndex + 1);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void getTinyInt(ResultSet rs, int colIndex) {
        try {
            rs.getByte(colIndex + 1);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void getSmallInt(ResultSet rs, int colIndex) {
        try {
            rs.getShort(colIndex + 1);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void getInt(ResultSet rs, int colIndex) {
        try {
            rs.getInt(colIndex + 1);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void getBigInt(ResultSet rs, int colIndex) {
        try {
            rs.getLong(colIndex + 1);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void getReal(ResultSet rs, int colIndex) {
        try {
            rs.getFloat(colIndex + 1);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void getDouble(ResultSet rs, int colIndex) {
        try {
            rs.getDouble(colIndex + 1);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void getDate(ResultSet rs, int colIndex) {
        try {
            rs.getDate(colIndex + 1);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void getDatetime(ResultSet rs, int colIndex) {
        try {
            rs.getTimestamp(colIndex + 1);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void getText(ResultSet rs, int colIndex) {
        try {
            rs.getString(colIndex + 1);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void setBool(PreparedStatement pstmt, int colIndex) {
        try {
            pstmt.setBoolean(colIndex + 1, true);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void setTinyInt(PreparedStatement pstmt, int colIndex) {
        try {
            pstmt.setByte(colIndex + 1, Byte.MAX_VALUE);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void setSmallInt(PreparedStatement pstmt, int colIndex) {
        try {
            pstmt.setShort(colIndex + 1, Short.MAX_VALUE);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void setInt(PreparedStatement pstmt, int colIndex) {
        try {
            pstmt.setInt(colIndex + 1, Integer.MAX_VALUE);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void setBigInt(PreparedStatement pstmt, int colIndex) {
        try {
            pstmt.setLong(colIndex + 1, Long.MAX_VALUE);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void setReal(PreparedStatement pstmt, int colIndex) {
        try {
            pstmt.setFloat(colIndex + 1, Float.MAX_VALUE);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void setDouble(PreparedStatement pstmt, int colIndex) {
        try {
            pstmt.setDouble(colIndex + 1, Double.MAX_VALUE);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void setDate(PreparedStatement pstmt, int colIndex) {
        try {
            pstmt.setDate(colIndex + 1, testDate);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void setDatetime(PreparedStatement pstmt, int colIndex) {
        try {
            pstmt.setTimestamp(colIndex + 1, testDateTime);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void setText(PreparedStatement pstmt, int colIndex) {
        try {
            pstmt.setString(colIndex + 1, testText);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private boolean isTextType(ColType type) {
        return type.equals(VARCHAR) || type.equals(NVARCHAR);
    }

    private long rowLength(ColType type, int colAmount, int textLength) {
        return isTextType(type) ?
                textLength * Byte.BYTES * colAmount :
                type.getSize() * colAmount;
    }

    private long rowLengthAll(int colAmountPerType) {
        long result = 0;
        for (ColType colType: ColType.values()) {
            for (int i = 0; i < colAmountPerType; i++) {
                result += isTextType(colType) ?
                        varcharSizeForSelectAll * Byte.BYTES :
                        colType.getSize();
            }
        }
        return result;
    }
}

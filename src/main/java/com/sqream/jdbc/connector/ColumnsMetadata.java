package com.sqream.jdbc.connector;

import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Set;
import java.util.stream.IntStream;

public class ColumnsMetadata {

    private String[] colNames;
    private String[] colTypes;
    private int[] colSizes;
    private BitSet colNullable;
    private BitSet colTvc;
    private HashMap<String, Integer> colNamesMap;

    public void init(int rowLength) {
        colNames = new String[rowLength];
        colTypes = new String[rowLength];
        colSizes = new int[rowLength];
        colNullable = new BitSet(rowLength);
        colTvc = new BitSet(rowLength);
        colNamesMap = new HashMap<>();
    }

    public void setByIndex(int index, ColumnMetadataDto colMetaData) {
        colNullable.set(index, colMetaData.isNullable());
        colTvc.set(index, colMetaData.isTruVarchar());
        colNames[index] = colMetaData.getName();
        colTypes[index] = colMetaData.getValueType();
        colSizes[index] = colMetaData.getValueSize();
        colNamesMap.put(colNames[index].toLowerCase(), index + 1);
    }

    String getName(int index) {
        return colNames[index];
    }

    String getType(int index) {
        return colTypes[index];
    }

    int getSize(int index) {
        return colSizes[index];
    }

    boolean isNullable(int index) {
        return colNullable.get(index);
    }

    boolean isTruVarchar(int index) {
        return colTvc.get(index);
    }

    int getAmountNullablleColumns() {
        return colNullable.cardinality();
    }

    int getSizesSum() {
        return IntStream.of(colSizes).sum();
    }

    int getMaxSize() {
        return Arrays.stream(colSizes).max().getAsInt();
    }

    Integer getColNumByName(String name) {
        return colNamesMap.get(name);
    }

    Set<String> getAllNames() {
        return colNamesMap.keySet();
    }
}

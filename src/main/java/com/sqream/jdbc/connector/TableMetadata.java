package com.sqream.jdbc.connector;

import com.sqream.jdbc.connector.enums.StatementType;

import java.util.*;
import java.util.stream.IntStream;

import static com.sqream.jdbc.connector.enums.StatementType.SELECT;

public class TableMetadata {

    private static final String DENIED = "denied";

    private String[] colNames;
    private String[] colTypes;
    private int[] colSizes;
    private BitSet colNullable;
    private BitSet colTvc;
    private HashMap<String, Integer> colNamesMap;
    private int rowLength;

    public void init(int rowLength) {
        this.rowLength = rowLength;
        colNames = new String[rowLength];
        colTypes = new String[rowLength];
        colSizes = new int[rowLength];
        colNullable = new BitSet(rowLength);
        colTvc = new BitSet(rowLength);
        colNamesMap = new HashMap<>();
    }

    public void setByIndex(int index, ColumnMetadataDto colMetadata) {
        colNullable.set(index, colMetadata.isNullable());
        colTvc.set(index, colMetadata.isTruVarchar());
        colNames[index] = colMetadata.getName();
        colTypes[index] = colMetadata.getValueType();
        colSizes[index] = colMetadata.getValueSize();
        colNamesMap.put(colNames[index].toLowerCase(), index + 1);
    }

    public void set(List<ColumnMetadataDto> metadataDtos, StatementType statementType) {
        for(int i=0; i < metadataDtos.size(); i++) {
            ColumnMetadataDto colMetaDataDto = metadataDtos.get(i);
            if (!statementType.equals(SELECT)) {
                colMetaDataDto.setName(DENIED);
            }
            setByIndex(i, colMetaDataDto);
        }
    }

    String getName(int index) {
        return colNames[index];
    }

    public int getRowLength() {
        return rowLength;
    }

    public String getType(int index) {
        return colTypes[index];
    }

    public int getSize(int index) {
        return colSizes[index];
    }

    public boolean isNullable(int index) {
        return colNullable.get(index);
    }

    public boolean isTruVarchar(int index) {
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

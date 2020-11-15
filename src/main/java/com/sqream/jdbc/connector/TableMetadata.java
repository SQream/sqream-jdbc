package com.sqream.jdbc.connector;

import com.sqream.jdbc.connector.enums.StatementType;

import java.text.MessageFormat;
import java.util.*;
import java.util.stream.IntStream;

import static com.sqream.jdbc.connector.enums.StatementType.SELECT;

public class TableMetadata {

    private static final String DENIED = "denied";

    private String[] colNames;
    private String[] colTypes;
    private int[] colSizes;
    private int[] colScales;
    private BitSet colNullable;
    private BitSet colTvc;
    private HashMap<String, Integer> colNamesMap;
    private int rowLength;

    private TableMetadata(int rowLength) {
        this.rowLength = rowLength;
        colNames = new String[rowLength];
        colTypes = new String[rowLength];
        colSizes = new int[rowLength];
        colScales = new int[rowLength];
        colNullable = new BitSet(rowLength);
        colTvc = new BitSet(rowLength);
        colNamesMap = new HashMap<>();
    }

    public static WithRowLength builder () {
        return new TableMetadataBuilder();
    }

    private void setByIndex(int index, ColumnMetadataDto colMetadata) {
        colNullable.set(index, colMetadata.isNullable());
        colTvc.set(index, colMetadata.isTruVarchar());
        colNames[index] = colMetadata.getName();
        colTypes[index] = colMetadata.getValueType();
        colSizes[index] = colMetadata.getValueSize();
        colScales[index] = colMetadata.getScale();
        colNamesMap.put(colNames[index].toLowerCase(), index + 1);
    }

    private void init(List<ColumnMetadataDto> metadataDtos, StatementType statementType) {
        if (metadataDtos == null || statementType == null) {
            throw new IllegalArgumentException(MessageFormat.format(
                    "ColumnMetadataList or statementType can not be null: metadataDtos=[{0}], statementType=[{1}]",
                    metadataDtos, statementType));
        }

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

    public int getScale(int index) {
        return colScales[index];
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

    private static class TableMetadataBuilder implements WithRowLength, WithColumnMetadataList, WithStatementType, TableMetadataCreator {

        private int rowLength;
        private StatementType statementType;
        private List<ColumnMetadataDto> metadataDtos;

        @Override
        public WithColumnMetadataList rowLength(int rowLength) {
            this.rowLength = rowLength;
            return this;
        }

        @Override
        public WithStatementType fromColumnsMetadata(List<ColumnMetadataDto> metadataDtos) {
            this.metadataDtos = metadataDtos;
            return this;
        }

        @Override
        public TableMetadataBuilder statementType(StatementType statementType) {
            this.statementType = statementType;
            return this;
        }

        @Override
        public TableMetadata build() {
            TableMetadata result = new TableMetadata(rowLength);
            result.init(metadataDtos, statementType);
            return result;
        }
    }

    public interface WithRowLength {
        WithColumnMetadataList rowLength(int rowLength);
    }

    public interface WithColumnMetadataList {

        WithStatementType fromColumnsMetadata(List<ColumnMetadataDto> metadataDtos);
    }

    public interface WithStatementType {
        TableMetadataCreator statementType(StatementType statementType);
    }

    public interface TableMetadataCreator {
        TableMetadata build();
    }
}

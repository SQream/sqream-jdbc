package com.sqream.jdbc.catalogQueryBuilder;

public class PatternSupportedCatalogQueryBuilder extends AbstractCatalogQueryBuilder {
    private static final String NULL_REPLACEMENT = "%";
    private static final String GET_TABLES_UF = "get_tables_like";
    private static final String GET_COLUMNS_UF = "get_columns_like";
    private static final String EMPTY_TYPE_LIST_REPLACEMENT = "";

    @Override
    protected String getNullReplacement() {
        return NULL_REPLACEMENT;
    }

    @Override
    String getTablesUF() {
        return GET_TABLES_UF;
    }

    @Override
    String getColumnsUF() {
        return GET_COLUMNS_UF;
    }

    @Override
    String emptyTypeListReplacement() {
        return EMPTY_TYPE_LIST_REPLACEMENT;
    }
}

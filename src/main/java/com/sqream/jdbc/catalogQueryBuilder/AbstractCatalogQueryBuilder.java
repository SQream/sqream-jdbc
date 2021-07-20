package com.sqream.jdbc.catalogQueryBuilder;

import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public abstract class AbstractCatalogQueryBuilder implements CatalogQueryBuilder {
    private static final Set<String> SUPPORTED_TABLE_TYPES = new HashSet<>(Arrays.asList("TABLE", "VIEW", "EXTERNAL_TABLE"));

    @Override
    public String getTables(String catalog,
                            String schemaPattern,
                            String tableNamePattern,
                            String[] types,
                            String currentDb) throws SQLException {

        String strTypes = types != null && types.length > 0 ? toTypesString(types) : emptyTypeListReplacement();
        if (catalog == null) {
            catalog = currentDb;
        }
        return MessageFormat.format("select {0}(''{1}'',''{2}'',''{3}'',''{4}'')",
                getTablesUF(),
                replaceNullOrTrim(unescapeIfNeeded(replaceWildcard(catalog))),
                replaceNullOrTrim(unescapeIfNeeded(replaceWildcard(schemaPattern))),
                replaceNullOrTrim(unescapeIfNeeded(replaceWildcard(tableNamePattern))),
                strTypes);
    }

    @Override
    public String getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern, String currentDatabase) {
        if (catalog == null) {
            catalog = currentDatabase;
        }
        String sql = MessageFormat.format("select {0}(''{1}'',''{2}'',''{3}'',''{4}'')",
                getColumnsUF(),
                replaceNullOrTrim(unescapeIfNeeded(replaceWildcard(catalog))),
                replaceNullOrTrim(unescapeIfNeeded(replaceWildcard(schemaPattern))),
                replaceNullOrTrim(unescapeIfNeeded(replaceWildcard(tableNamePattern))),
                replaceNullOrTrim(unescapeIfNeeded(replaceWildcard(columnNamePattern))));
        return sql.toLowerCase();
    }

    @Override
    public String getSchemas() {
        return "select get_schemas()";
    }

    @Override
    public String getTypeInfo() {
        return "select get_type_info()";
    }

    @Override
    public String getTableTypes() {
        return "select get_table_types()";
    }

    @Override
    public String getCatalogs() {
        return "select get_catalogs()";
    }

    @Override
    public String getProcedures() {
        return "select database_name as PROCEDURE_CAT, null as PROCEDURE_SCHEM, function_name as PROCEDURE_NAME, null as UNUSED, null as UNUSED2, null as UNUSED3, ' ' as REMARKS, 0 as PROCEDURE_TYPE, function_name as SPECIFIC_NAME from sqream_catalog.user_defined_functions";
    }

    private String replaceNullOrTrim(String str) {
        return str == null ? getNullReplacement() : str.trim();
    }

    abstract String getNullReplacement();

    abstract String getTablesUF();

    abstract String getColumnsUF();

    abstract String emptyTypeListReplacement();

    String unescapeIfNeeded(String str) {
        return str;
    }

    private String replaceWildcard(String str) {
        return str != null && str.equals("%") ? getNullReplacement() : str;
    }

    private String toTypesString(String[] types) throws SQLException {
        String[] typesUpper = types.clone();
        for (int i = 0; i < typesUpper.length; i++) {
            typesUpper[i] = types[i].toUpperCase();
        }
        Set<String> typeSet = new HashSet<>(Arrays.asList(typesUpper));
        String previousSeparator = "";
        StringBuilder typesBuilder = new StringBuilder();
        for (String type : typeSet) {
            if (type.equals("SYNONYM")) {
                continue;
            }
            if (!SUPPORTED_TABLE_TYPES.contains(type)) {
                throw new SQLException(MessageFormat.format("Unsupported type [{0}] in types array {1}",
                        type, Arrays.asList(types)));
            }
            
            if (type.equals("EXTERNAL_TABLE")) {
                type = "EXTERNAL";
            }

            typesBuilder.append(previousSeparator);
            typesBuilder.append(type.toLowerCase());
            previousSeparator = ",";
        }
        return typesBuilder.toString();
    }
}

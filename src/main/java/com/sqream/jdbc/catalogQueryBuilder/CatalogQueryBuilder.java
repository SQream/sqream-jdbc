package com.sqream.jdbc.catalogQueryBuilder;

import java.sql.SQLException;

public interface CatalogQueryBuilder {

    String getTables(String catalog, String schemaPattern, String tableNamePattern,
                     String[] types, String currentDatabase) throws SQLException;

    String getColumns(String catalog, String schemaPattern, String tableNamePattern,
                      String columnNamePattern, String currentDatabase);

    String getSchemas();

    String getTypeInfo();

    String getTableTypes();

    String getCatalogs();

    String getProcedures();
}

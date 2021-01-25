package com.sqream.jdbc.catalogQueryBuilder;

import com.sqream.jdbc.connector.ServerVersion;

public class CatalogQueryBuilderFactory {
    private static final String SQL_PATTERN_SUPPORTED_VERSION = "2021.1";

    public CatalogQueryBuilder createBuilder(String serverVersion) {
        if (serverVersion != null && serverVersion.length() > 0 &&
                ServerVersion.compare(serverVersion, SQL_PATTERN_SUPPORTED_VERSION) >= 0) {
            return new PatternSupportedCatalogQueryBuilder();
        }
        return new ExplicitCatalogQueryBuilder();
    }

}

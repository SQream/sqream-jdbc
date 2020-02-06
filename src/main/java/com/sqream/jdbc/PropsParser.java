package com.sqream.jdbc;

import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.function.Consumer;

public class PropsParser {

    public static Properties merge(Properties urlProps, Properties driverProps, Properties defaultProps) throws SQLException {
        if (urlProps == null || driverProps == null || defaultProps == null) {
            throw new IllegalArgumentException(MessageFormat.format(
                    "Properties should not be null: urlProps=[{0}], driverProps=[{1}], defaultProps=[{2}]",
                    urlProps, driverProps, defaultProps));
        }
        putIfAbsent(urlProps, driverProps);
        putIfAbsent(urlProps, defaultProps);
        return urlProps;
    }

    private static void putIfAbsent(Properties target, Properties values) {
        Set<String> keys = values.stringPropertyNames();
        for (String key : keys) {
            target.putIfAbsent(key, values.getProperty(key));
        }
    }
}

package com.sqream.jdbc.propsParser;

import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.Properties;
import java.util.Set;

public class PropsParser {

    private static final URLParser urlParser = new URLParser();

    public static Properties parse(String url) throws SQLException {
        return parse(url, new Properties());
    }

    public static CaselessProperties parse(String primary, Properties... additions) throws SQLException {
        CaselessProperties result = urlParser.parse(primary);
        for (Properties additional : additions) {
            merge(result, additional);
        }
        return result;
    }

    private static void merge(CaselessProperties primary, Properties additional) {
        if (primary == null || additional == null) {
            throw new IllegalArgumentException(MessageFormat.format(
                    "Properties should not be null: primary=[{0}], additional=[{1}]", primary, additional));
        }
        addIfAbsent(primary, additional);
    }

    private static void addIfAbsent(CaselessProperties target, Properties values) {
        Set<Object> keys = values.keySet();
        for (Object key : keys) {
            target.putIfAbsent(key, values.get(key));
        }
    }
}

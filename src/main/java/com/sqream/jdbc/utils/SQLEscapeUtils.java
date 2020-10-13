package com.sqream.jdbc.utils;

import java.util.HashMap;
import java.util.Map;

public class SQLEscapeUtils {
    private static final Map<String, String> map = new HashMap<>();

    static {
        map.put("\\n", "\n");
        map.put("\\b", "\b");
        map.put("\\r", "\r");
        map.put("\\%", "%");
        map.put("\\_", "_");
        map.put("\\t", "\t");
        map.put("\\\"", "\"");
        map.put("\\'", "'");
    }

    /**
     * Escapes special SQL characters
     */
    public static String escape(String str) {
        for (Map.Entry<String,String> entry : map.entrySet()) {
            str = str.replaceAll(String.format("\\%s",entry.getKey()), entry.getValue());
        }
        return str;
    }

}

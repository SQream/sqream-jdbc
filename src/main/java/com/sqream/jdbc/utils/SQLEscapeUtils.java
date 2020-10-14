package com.sqream.jdbc.utils;

import java.util.HashMap;
import java.util.Map;

public class SQLEscapeUtils {
    private static final Map<Character, Character> special = new HashMap<>();

    static {
        special.put('n', '\n');
        special.put('r', '\r');
        special.put('t', '\t');
    }

    /**
     * Unescapes special SQL characters
     */
    public static String unescape(String str) {
        StringBuilder newStr = new StringBuilder();

        boolean escape = false;
        for (int i = 0; i < str.length(); i++) {
            Character ch = str.charAt(i);
            if (escape) {
                newStr.append(special.getOrDefault(ch, ch));
                escape = false;
            } else if (ch.equals('\\')) {
                escape = true;
            } else {
                newStr.append(ch);
            }
        }
        return newStr.toString();
    }

}

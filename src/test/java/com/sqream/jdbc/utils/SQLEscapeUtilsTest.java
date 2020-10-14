package com.sqream.jdbc.utils;

import org.junit.Assert;
import org.junit.Test;

import java.text.MessageFormat;

public class SQLEscapeUtilsTest {

    @Test
    public void escapeTest() {
        testEscape("\\n", "\n");
        testEscape("\\r", "\r");
        testEscape("\\%", "%");
        testEscape("\\_", "_");
        testEscape("\\t", "\t");
        testEscape("\\\"", "\"");
        testEscape("\\'", "'");
    }

    private void testEscape(String toEscape, String escaped) {
        String origin = toEscape + toEscape + "some" + "thing" + toEscape + toEscape;
        String expected = escaped + escaped + "some" + "thing" + escaped + escaped;

        Assert.assertEquals(
                MessageFormat.format("Failed to replace [{0}] with [{1}]", toEscape, escaped),
                expected,
                SQLEscapeUtils.unescape(origin));
    }

    @Test
    public void specialCasesTest() {
        Assert.assertEquals("\n", SQLEscapeUtils.unescape("\\n"));
        Assert.assertEquals("\\n", SQLEscapeUtils.unescape("\\\\n"));
        Assert.assertEquals("\\\n", SQLEscapeUtils.unescape("\\\\\\n"));
        Assert.assertEquals("\\n\\", SQLEscapeUtils.unescape("\\\\n\\\\"));
        Assert.assertEquals("ab", SQLEscapeUtils.unescape("\\a\\b"));
        Assert.assertEquals("aaa", SQLEscapeUtils.unescape("aaa\\"));
    }
}

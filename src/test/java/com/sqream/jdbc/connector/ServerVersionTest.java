package com.sqream.jdbc.connector;

import org.junit.Test;

import static org.junit.Assert.*;

public class ServerVersionTest {
    @Test
    public void whenVersionIsNullThenThrowsException() {
        try {
            new ServerVersion(null);
            fail("Should throw exception if parameter is null");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("cannot be null"));
        }

        try {
            ServerVersion.compare(null, "1234.5.6");
            fail("Should throw exception if parameter is null");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Comparable version cannot be null"));
        }
    }

    @Test
    public void compareToTest() {
        assertEquals(0, ServerVersion.compare("1234.5.6", "1234.5.6"));
        assertEquals(0, ServerVersion.compare("1234.5", "1234.5.0"));
        // not applicable versions are equals
        assertEquals(0, ServerVersion.compare("version1", "version2"));

        assertEquals(1, ServerVersion.compare("1234.1", "1233.1"));
        assertEquals(1, ServerVersion.compare("1234.5", "1234.4"));
        assertEquals(1, ServerVersion.compare("1234.5", "1234.4.9"));
        assertEquals(1, ServerVersion.compare("1234.5.0", "1234.4.9"));
        assertEquals(1, ServerVersion.compare("1234.10.1", "1234.2.1"));
        assertEquals(1, ServerVersion.compare("1234.5.1.1", "1234.5.1"));
        // any applicable version is higher than not applicable
        assertEquals(1, ServerVersion.compare("1234.56", "version1"));
        assertEquals(-1, ServerVersion.compare("version1", "1234.56"));
        assertEquals(-1, ServerVersion.compare("1233.5.1.1", "1234.5.1"));
        assertEquals(-1, ServerVersion.compare("", "1234.56"));
        assertEquals(-1, ServerVersion.compare("1234", "1234.1"));
        assertEquals(1, ServerVersion.compare("1235", "1234"));
    }
}

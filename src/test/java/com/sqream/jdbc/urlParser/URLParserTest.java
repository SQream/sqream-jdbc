package com.sqream.jdbc.urlParser;

import org.junit.Test;

import java.net.URISyntaxException;
import java.sql.SQLException;

import static org.junit.Assert.*;

public class URLParserTest {
    private static final String TEST_URL =
            "jdbc:Sqream://127.0.0.1:1234/testDataBaseName;" +
                    "user=testUserName;password=testPassword;cluster=true;ssl=true;service=testService;" +
                    "loggerLevel=testLoggerLevel;logFile=testLogFilePath";

    private URLParser parser = new URLParser();

    @Test
    public void parseProviderTest() throws SQLException {
        String expected = "sqream";

        String actual = parser.parse(TEST_URL).getProvider().toLowerCase();

        assertEquals(expected, actual);
    }

    @Test
    public void parseHostTest() throws SQLException {
        String expected = "127.0.0.1";

        String actual = parser.parse(TEST_URL).getHost();

        assertEquals(expected, actual);
    }

    @Test
    public void parsePortTest() throws SQLException {
        int expected = 1234;

        int actual = parser.parse(TEST_URL).getPort();

        assertEquals(expected, actual);
    }

    @Test
    public void parseDataBaseTest() throws SQLException {
        String expected = "testDataBaseName";

        String actual = parser.parse(TEST_URL).getDbName();

        assertEquals(expected, actual);
    }

    @Test
    public void parseUserTest() throws SQLException {
        String expected = "testUserName";

        String actual = parser.parse(TEST_URL).getUser();

        assertEquals(expected, actual);
    }

    @Test
    public void parsePasswordTest() throws SQLException {
        String expected = "testPassword";

        String actual = parser.parse(TEST_URL).getPswd();

        assertEquals(expected, actual);
    }

    @Test
    public void parseClusterTest() throws SQLException {
        String expected = "true";

        String actual = parser.parse(TEST_URL).getCluster();

        assertEquals(expected, actual);
    }

    @Test
    public void parseSslTest() throws SQLException {
        String expected = "true";

        String actual = parser.parse(TEST_URL).getSsl();

        assertEquals(expected, actual);
    }

    @Test
    public void parseServiceTest() throws SQLException {
        String expected = "testService";

        String actual = parser.parse(TEST_URL).getService();

        assertEquals(expected, actual);
    }

    @Test
    public void parseLoggerLevelTest() throws SQLException {
        String expected = "testLoggerLevel";

        String actual = parser.parse(TEST_URL).getLoggerLevel();

        assertEquals(expected, actual);
    }

    @Test
    public void parseLogFilePathTest() throws SQLException {
        String expected = "testLogFilePath";

        String actual = parser.parse(TEST_URL).getLogFilePath();

        assertEquals(expected, actual);
    }
}
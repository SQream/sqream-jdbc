package com.sqream.jdbc.propsParser;

import com.sqream.jdbc.URLParser;
import org.junit.Test;

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
        String expected = "Sqream";

        String actual = parser.parse(TEST_URL).getProperty("provider");

        assertEquals(expected, actual);
    }

    @Test
    public void parseHostTest() throws SQLException {
        String expected = "127.0.0.1";

        String actual = parser.parse(TEST_URL).getProperty("host");

        assertEquals(expected, actual);
    }

    @Test
    public void parsePortTest() throws SQLException {
        int expected = 1234;

        int actual = Integer.parseInt(parser.parse(TEST_URL).getProperty("port"));

        assertEquals(expected, actual);
    }

    @Test
    public void parseDataBaseTest() throws SQLException {
        String expected = "testDataBaseName";

        String actual = parser.parse(TEST_URL).getProperty("dbName");

        assertEquals(expected, actual);
    }

    @Test
    public void parseUserTest() throws SQLException {
        String expected = "testUserName";

        String actual = parser.parse(TEST_URL).getProperty("user");

        assertEquals(expected, actual);
    }

    @Test
    public void parsePasswordTest() throws SQLException {
        String expected = "testPassword";

        String actual = parser.parse(TEST_URL).getProperty("password");

        assertEquals(expected, actual);
    }

    @Test
    public void parseClusterTest() throws SQLException {
        String expected = "true";

        String actual = parser.parse(TEST_URL).getProperty("cluster");

        assertEquals(expected, actual);
    }

    @Test
    public void parseSslTest() throws SQLException {
        String expected = "true";

        String actual = parser.parse(TEST_URL).getProperty("ssl");

        assertEquals(expected, actual);
    }

    @Test
    public void parseServiceTest() throws SQLException {
        String expected = "testService";

        String actual = parser.parse(TEST_URL).getProperty("service");

        assertEquals(expected, actual);
    }

    @Test
    public void parseLoggerLevelTest() throws SQLException {
        String expected = "testLoggerLevel";

        String actual = parser.parse(TEST_URL).getProperty("loggerLevel");

        assertEquals(expected, actual);
    }

    @Test
    public void parseLogFilePathTest() throws SQLException {
        String expected = "testLogFilePath";

        String actual = parser.parse(TEST_URL).getProperty("logFile");

        assertEquals(expected, actual);
    }
}
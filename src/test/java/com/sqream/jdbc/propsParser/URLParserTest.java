package com.sqream.jdbc.propsParser;

import com.sqream.jdbc.enums.DriverProperties;
import org.junit.Test;

import java.sql.SQLException;

import static com.sqream.jdbc.enums.DriverProperties.*;
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

        String actual = parser.parse(TEST_URL).getProperty(PROVIDER.getValue());

        assertEquals(expected, actual);
    }

    @Test
    public void parseHostTest() throws SQLException {
        String expected = "127.0.0.1";

        String actual = parser.parse(TEST_URL).getProperty(HOST.getValue());

        assertEquals(expected, actual);
    }

    @Test
    public void parsePortTest() throws SQLException {
        int expected = 1234;

        int actual = Integer.parseInt(parser.parse(TEST_URL).getProperty(PORT.getValue()));

        assertEquals(expected, actual);
    }

    @Test
    public void parseDataBaseTest() throws SQLException {
        String expected = "testDataBaseName";

        String actual = parser.parse(TEST_URL).getProperty(DB_NAME.getValue());

        assertEquals(expected, actual);
    }

    @Test
    public void parseUserTest() throws SQLException {
        String expected = "testUserName";

        String actual = parser.parse(TEST_URL).getProperty(USER.getValue());

        assertEquals(expected, actual);
    }

    @Test
    public void parsePasswordTest() throws SQLException {
        String expected = "testPassword";

        String actual = parser.parse(TEST_URL).getProperty(PASSWORD.getValue());

        assertEquals(expected, actual);
    }

    @Test
    public void parseClusterTest() throws SQLException {
        String expected = "true";

        String actual = parser.parse(TEST_URL).getProperty(CLUSTER.getValue());

        assertEquals(expected, actual);
    }

    @Test
    public void parseSslTest() throws SQLException {
        String expected = "true";

        String actual = parser.parse(TEST_URL).getProperty(SSL.getValue());

        assertEquals(expected, actual);
    }

    @Test
    public void parseServiceTest() throws SQLException {
        String expected = "testService";

        String actual = parser.parse(TEST_URL).getProperty(SERVICE.getValue());

        assertEquals(expected, actual);
    }

    @Test
    public void parseLoggerLevelTest() throws SQLException {
        String expected = "testLoggerLevel";

        String actual = parser.parse(TEST_URL).getProperty(LOGGER_LEVEL.getValue());

        assertEquals(expected, actual);
    }

    @Test
    public void parseLogFilePathTest() throws SQLException {
        String expected = "testLogFilePath";

        String actual = parser.parse(TEST_URL).getProperty(LOG_FILE_PATH.getValue());

        assertEquals(expected, actual);
    }
}
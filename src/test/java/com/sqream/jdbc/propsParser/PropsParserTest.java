package com.sqream.jdbc.propsParser;

import com.sqream.jdbc.TestEnvironment;
import org.junit.Test;

import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.Properties;

import static com.sqream.jdbc.TestEnvironment.SHORT_URL;
import static com.sqream.jdbc.TestEnvironment.URL;
import static com.sqream.jdbc.enums.DriverProperties.*;
import static org.junit.Assert.*;

public class PropsParserTest {

    private static final String LOG_LEVEL ="testLoggerLevel";
    private static final String LOG_FILE ="testLogFilePath";
    private static final String FULL_URL =
            MessageFormat.format("{0};loggerLevel={1};logFile={2}", URL, LOG_LEVEL, LOG_FILE);

    @Test
    public void primaryDriverPropertiesRewriteUrlPropertiesTest() throws SQLException {
        Properties driverProps = new Properties();
        driverProps.setProperty(USER.getValue(), "newTestUserName");
        driverProps.setProperty(PASSWORD.getValue(), "newTestPassword");
        driverProps.setProperty(CLUSTER.getValue(), "newClusterTestValue");
        driverProps.setProperty(SSL.getValue(), "newSslTestValue");
        driverProps.setProperty(SERVICE.getValue(), "newTestService");
        driverProps.setProperty(LOGGER_LEVEL.getValue(), "newTestLoggerLevel");
        driverProps.setProperty(LOG_FILE_PATH.getValue(), "newTestLogFilePath");

        Properties result = PropsParser.parse(driverProps, FULL_URL);

        assertEquals(driverProps.getProperty(USER.getValue()), result.getProperty(USER.getValue()));
        assertEquals(driverProps.getProperty(PASSWORD.toString()), result.getProperty(PASSWORD.getValue()));
        assertEquals(driverProps.getProperty(CLUSTER.getValue()), result.getProperty(CLUSTER.getValue()));
        assertEquals(driverProps.getProperty(SSL.getValue()), result.getProperty(SSL.getValue()));
        assertEquals(driverProps.getProperty(SERVICE.getValue()), result.getProperty(SERVICE.getValue()));
        assertEquals(driverProps.getProperty(LOGGER_LEVEL.getValue()), result.getProperty(LOGGER_LEVEL.getValue()));
        assertEquals(driverProps.getProperty(LOG_FILE_PATH.getValue()), result.getProperty(LOG_FILE_PATH.getValue()));
    }

    @Test
    public void defaultValueSetIfNotExist() throws SQLException {
        Properties defaultProps = new Properties();
        defaultProps.setProperty(CLUSTER.toString(), "false");
        defaultProps.setProperty(SSL.toString(), "false");
        defaultProps.setProperty(SERVICE.toString(), "sqream");
        defaultProps.setProperty(SCHEMA.toString(), "public");

        Properties result = PropsParser.parse(defaultProps, SHORT_URL);

        assertEquals("false", result.getProperty(CLUSTER.getValue()));
        assertEquals("false", result.getProperty(SSL.getValue()));
        assertEquals("sqream", result.getProperty(SERVICE.getValue()));
        assertEquals("public", result.getProperty(SCHEMA.getValue()));
    }
}
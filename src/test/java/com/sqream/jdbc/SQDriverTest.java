package com.sqream.jdbc;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.sql.*;
import java.util.Properties;
import java.util.logging.Level;

import static org.junit.Assert.*;

@RunWith(MockitoJUnitRunner.class)
public class SQDriverTest {
    private static final String CORRECT_URI = "jdbc:Sqream://127.0.0.1:5000/master;user=sqream;password=sqream";
    private static final String LOWER_CASE_PROVIDER_URI = "jdbc:sqream://127.0.0.1:5000/master;user=sqream;password=sqream";
    private static final String ANOTHER_PROVIDER_URI = "jdbc:SomeProvider://127.0.0.1:5000/master;user=sqream;password=sqream";

    private static final int MINOR_VERSION = 0;
    private static final int MAJOR_VERSION = 4;

    private static final Properties CORRECT_CONN_PROPERTIES = new Properties();

    private Driver driver = new SQDriver();

    // init connection properties
    static {
        CORRECT_CONN_PROPERTIES.setProperty("user", "sqream");
        CORRECT_CONN_PROPERTIES.setProperty("password", "password");
    }

    @Test
    public void whenCorrectUriAcceptsURLReturnTrue() throws SQLException {
        assertTrue(driver.acceptsURL(CORRECT_URI));
    }

    @Test(expected = SQLException.class)
    public void whenUriIsNullAcceptsURLThowExceptionTest() throws SQLException {
        driver.acceptsURL(null);
    }

    @Test(expected = SQLException.class)
    public void whenUriIsEmptyAcceptsURLThowExceptionTest() throws SQLException {
        driver.acceptsURL("");
    }

    @Test
    public void whenAnotherProviderInUriAcceptsURLReturnFalse() throws SQLException {
        assertFalse(driver.acceptsURL(ANOTHER_PROVIDER_URI));
    }

    @Test
    public void getMinorVersionTest() {
        assertEquals(MINOR_VERSION, driver.getMinorVersion());
    }

    @Test
    public void getMajorVersionTest() {
        assertEquals(MAJOR_VERSION, driver.getMajorVersion());
    }

    @Test
    public void jdbcCompliantTest() {
        assertTrue(driver.jdbcCompliant());
    }

    @Test
    public void getParentLoggerTest() throws SQLFeatureNotSupportedException {
        assertNotNull(driver.getParentLogger());
    }

    @Test
    public void getPropertyInfoTest() throws SQLException {
        DriverPropertyInfo[] propertyInfo = driver.getPropertyInfo(null, null);
        assertEquals(0, propertyInfo.length);
    }

    @Test(expected = SQLException.class)
    public void whenProviderLowerCaseConnectThrowExceptionTest() throws SQLException {
        driver.connect(LOWER_CASE_PROVIDER_URI, CORRECT_CONN_PROPERTIES);
    }

    @Test(expected = SQLException.class)
    public void whenPropertiesIsNullConnectThrowExceptionTest() throws SQLException {
        driver.connect(CORRECT_URI, null);
    }

    @Test
    public void whenPropertiesDoNotContainUserOrPasswordConnectReturnConnectionTest() throws SQLException {
        Connection connection = driver.connect(CORRECT_URI, new Properties());
        assertNotNull(connection);
    }

    @Test
    public void loggerLevelIsOffTest() throws SQLException {
        driver.connect(CORRECT_URI, new Properties());
        Level expectedLevel = driver.getParentLogger().getLevel();
        assertEquals(expectedLevel, Level.OFF);
    }

    @Test
    public void loggerLevelDebugTest() throws SQLException {
        driver.connect(CORRECT_URI + "?loggerLevel=DEBUG", new Properties());
        Level expectedLevel = driver.getParentLogger().getLevel();
        assertEquals(expectedLevel, Level.FINE);
    }

    @Test
    public void loggerLevelTraceTest() throws SQLException {
        driver.connect(CORRECT_URI + "?loggerLevel=TRACE", new Properties());
        Level expectedLevel = driver.getParentLogger().getLevel();
        assertEquals(expectedLevel, Level.FINEST);
    }

    @Test
    public void loggerLevelWrongParamKeyIgnoredTest() throws SQLException {
        driver.connect(CORRECT_URI + "?loggerWronKeyLevel=DEBUG", new Properties());
        Level expectedLevel = driver.getParentLogger().getLevel();
        assertEquals(expectedLevel, Level.OFF);
    }

    @Test(expected = IllegalArgumentException.class)
    public void loggerLevelUnsupportedLevel() throws SQLException {
        driver.connect(CORRECT_URI + "?loggerLevel=UNSUPPORTED_LEVEL", new Properties());
    }
}
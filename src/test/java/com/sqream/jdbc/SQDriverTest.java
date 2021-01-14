package com.sqream.jdbc;


import java.io.File;
import java.text.MessageFormat;
import static com.sqream.jdbc.TestEnvironment.URL;

import com.sqream.jdbc.connector.*;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import javax.script.ScriptException;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.sql.*;
import java.util.Properties;
import java.util.logging.Handler;
import java.util.logging.Level;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;

@RunWith(MockitoJUnitRunner.class)
public class SQDriverTest {
    private static final String CORRECT_URI =
            "jdbc:Sqream://127.0.0.1:5000/master;user=sqream;password=sqream;ssl=true";
    private static final String LOWER_CASE_PROVIDER_URI =
            "jdbc:sqream://127.0.0.1:5000/master;user=sqream;password=sqream";
    private static final String ANOTHER_PROVIDER_URI =
            "jdbc:SomeProvider://127.0.0.1:5000/master;user=sqream;password=sqream";

    private static final int MINOR_VERSION = 0;
    private static final int MAJOR_VERSION = 4;

    private static final Properties CORRECT_CONN_PROPERTIES = new Properties();

    // init connection properties
    static {
        CORRECT_CONN_PROPERTIES.setProperty("user", "sqream");
        CORRECT_CONN_PROPERTIES.setProperty("password", "password");
    }

    @Mock
    private ConnectorFactory connectorFactoryMock;

	@Test
    public void logfileLockTest() throws SQLException {
        mockConnector();
        String TEMP_DIR = "logs_temp";
        int connAmount = 200;
        File dir = new File(TEMP_DIR);
        if (!dir.exists()) {
            boolean mkdir = new File(TEMP_DIR).mkdir();
            assertTrue("Can not create temp folder for logs", mkdir);
        }

        Driver driver = new DriverTest();
        for (int i = 0; i < connAmount; i++) {
            driver.connect(
                    MessageFormat.format("{0};loggerLevel=DEBUG;logFile={1}/test.log", URL, TEMP_DIR),
                    new Properties());
        }
        // necessary to remove lock from file
        for (Handler handler : driver.getParentLogger().getHandlers()) {
            handler.close();
        }

        File[] files = dir.listFiles();
        assertNotNull("Temp folder should contains at least one log file", files);
        assertEquals("Logs should be written in one file", 1, files.length);
        for (File file : files) {
            boolean rmLogFile = file.delete();
            assertTrue("Can not remove log file: " + file.getName(), rmLogFile);
        }
        boolean rmdir = dir.delete();
        assertTrue("Can not remove temp folder", rmdir);
    }

    @Test
    public void whenCorrectUriAcceptsURLReturnTrue() throws SQLException {
        assertTrue(new DriverTest().acceptsURL(CORRECT_URI));
    }

    @Test(expected = SQLException.class)
    public void whenUriIsNullAcceptsURLThowExceptionTest() throws SQLException {
        new DriverTest().acceptsURL(null);
    }

    @Test(expected = SQLException.class)
    public void whenUriIsEmptyAcceptsURLThowExceptionTest() throws SQLException {
        new DriverTest().acceptsURL("");
    }

    @Test
    public void whenAnotherProviderInUriAcceptsURLReturnFalse() throws SQLException {
        assertFalse(new DriverTest().acceptsURL(ANOTHER_PROVIDER_URI));
    }

    public void whenConnectWithAnotherProviderReturnNullTest() throws SQLException {
        Connection conn = new DriverTest().connect(ANOTHER_PROVIDER_URI, CORRECT_CONN_PROPERTIES);
        assertNull(conn);
    }

    @Test
    public void getMinorVersionTest() {
        assertEquals(MINOR_VERSION, new DriverTest().getMinorVersion());
    }

    @Test
    public void getMajorVersionTest() {
        assertEquals(MAJOR_VERSION, new DriverTest().getMajorVersion());
    }

    @Test
    public void jdbcCompliantTest() {
        assertTrue(new DriverTest().jdbcCompliant());
    }

    @Test
    public void getParentLoggerTest() throws SQLFeatureNotSupportedException {
        assertNotNull(new DriverTest().getParentLogger());
    }

    @Test
    public void getPropertyInfoTest() throws SQLException {
        DriverPropertyInfo[] propertyInfo = new DriverTest().getPropertyInfo(null, null);
        assertEquals(0, propertyInfo.length);
    }

    @Test
    public void whenProviderLowerCaseConnectReturnNullTest() throws SQLException {
        Connection conn = new DriverTest().connect(LOWER_CASE_PROVIDER_URI, CORRECT_CONN_PROPERTIES);
        assertNull(conn);
    }

    @Test(expected = SQLException.class)
    public void whenPropertiesIsNullConnectThrowExceptionTest() throws SQLException {
        new DriverTest().connect(CORRECT_URI, null);
    }

    @Test
    public void whenPropertiesDoNotContainUserOrPasswordConnectReturnConnectionTest() throws SQLException {
        mockConnector();
        Connection connection = new DriverTest().connect(CORRECT_URI, new Properties());
        assertNotNull(connection);
    }

    @Test
    public void loggerLevelIsOffTest() throws SQLException {
        mockConnector();
        new DriverTest().connect(CORRECT_URI, new Properties());
        Level expectedLevel = new DriverTest().getParentLogger().getLevel();
        assertEquals(expectedLevel, Level.OFF);
    }

    @Test
    public void loggerLevelDebugTest() throws SQLException {
        mockConnector();
        Driver driver = new DriverTest();
        driver.connect(CORRECT_URI + ";loggerLevel=DEBUG", new Properties());
        Level expectedLevel = driver.getParentLogger().getLevel();
        assertEquals(expectedLevel, Level.FINE);
    }

    @Test
    public void loggerLevelTraceTest() throws SQLException, IOException, ScriptException, NoSuchAlgorithmException, KeyManagementException {
        mockConnector();
        Driver driver = new DriverTest();
        driver.connect(CORRECT_URI + ";loggerLevel=TRACE", new Properties());
        Level expectedLevel = driver.getParentLogger().getLevel();
        assertEquals(expectedLevel, Level.FINEST);
    }

    @Test
    public void loggerLevelWrongParamKeyIgnoredTest() throws SQLException {
        mockConnector();
        Driver driver = new DriverTest();
        driver.connect(CORRECT_URI + ";loggerWrongKeyLevel=DEBUG", new Properties());
        Level expectedLevel = driver.getParentLogger().getLevel();
        assertEquals(expectedLevel, Level.OFF);
    }

    @Test(expected = IllegalArgumentException.class)
    public void loggerLevelUnsupportedLevel() throws SQLException {
        new DriverTest().connect(CORRECT_URI + ";loggerLevel=UNSUPPORTED_LEVEL", new Properties());
    }

    private void mockConnector() {
	    try {
            Connector connectorMock = Mockito.mock(ConnectorImpl.class);
            Mockito.when(connectorFactoryMock.createConnector(any(ConnectionParams.class)))
                    .thenReturn(connectorMock);
        } catch (ConnException e) {
            throw new RuntimeException(e);
        }
    }

    private class DriverTest extends SQDriver {

        @Override
        protected ConnectorFactory getConnectorFactory() {
            return connectorFactoryMock;
        }
    }
}

package com.sqream.jdbc;

import org.junit.Test;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.Properties;

import static com.sqream.jdbc.TestEnvironment.*;
import static org.junit.Assert.*;

public class SQConnectionTest {

    @Test
    public void getSchemaByDefaultTest() throws SQLException {
        String DEFAULT_SCHEMA = "public";
        Driver driver = new SQDriver();
        Connection conn = driver.connect(URL, new Properties());

        String schema = conn.getSchema();

        assertEquals(DEFAULT_SCHEMA, schema);
    }

    @Test
    public void getSchemaTest() throws SQLException {
        String expectedSchema = "expectedSchema";
        Driver driver = new SQDriver();
        Properties props = new Properties();
        props.setProperty("schema", expectedSchema);
        Connection conn = driver.connect(URL, props);

        String schema = conn.getSchema();

        assertEquals(expectedSchema, schema);
    }

    @Test
    public void whenPassedWrongAuthThenReturnCorrectStateTest() {
        String WRONG_USER = "wrong_user";
        String WRONG_PASS = "wrong_password";
        String URL_WITH_WRONG_USER = MessageFormat.format(
                "jdbc:Sqream://{0}:{1}/{2};user={3};password={4};cluster={5};ssl={6};service={7}",
                IP, String.valueOf(PORT), DATABASE, WRONG_USER, PASS, CLUSTER, SSL, SERVICE);
        String URL_WITH_WRONG_PASS = MessageFormat.format(
                "jdbc:Sqream://{0}:{1}/{2};user={3};password={4};cluster={5};ssl={6};service={7}",
                IP, String.valueOf(PORT), DATABASE, USER, WRONG_PASS, CLUSTER, SSL, SERVICE);
        String expectedCode = "28000";

        try (Connection conn = DriverManager.getConnection(URL_WITH_WRONG_USER, new Properties())) {
        } catch (SQLException e) {
            assertEquals(expectedCode, e.getSQLState());
        }

        try (Connection conn = DriverManager.getConnection(URL_WITH_WRONG_PASS, new Properties())) {
        } catch (SQLException e) {
            assertEquals(expectedCode, e.getSQLState());
        }
    }

    @Test
    public void whenPassedWrongDatabaseThenReturnCorrectStateTest() {
        String WRONG_DB_NAME = "wrong_password";
        String URL_WITH_WRONG_DB = MessageFormat.format(
                "jdbc:Sqream://{0}:{1}/{2};user={3};password={4};cluster={5};ssl={6};service={7}",
                IP, String.valueOf(PORT), WRONG_DB_NAME, USER, PASS, CLUSTER, SSL, SERVICE);

        String expectedCode = "3D000";

        try (Connection conn = DriverManager.getConnection(URL_WITH_WRONG_DB, new Properties())) {
        } catch (SQLException e) {
            assertEquals(expectedCode, e.getSQLState());
        }
    }
}
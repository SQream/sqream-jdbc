package com.sqream.jdbc;

import org.junit.Test;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.Properties;

import static com.sqream.jdbc.TestEnvironment.URL;
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
}
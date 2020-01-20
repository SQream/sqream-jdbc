package com.sqream.jdbc;

import org.junit.Test;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.Properties;

import static org.junit.Assert.*;

public class SQConnectionTest {

    @Test
    public void getSchemaByDefaultTest() throws SQLException {
        String DEFAULT_SCHEMA = "public";
        final String url = "jdbc:Sqream://127.0.0.1:5000/master;user=sqream;password=sqream;cluster=false;ssl=false;service=sqream";
        Driver driver = new SQDriver();
        Connection conn = driver.connect(url, new Properties());

        String schema = conn.getSchema();

        assertEquals(DEFAULT_SCHEMA, schema);
    }

    @Test
    public void getSchemaTest() throws SQLException {
        String expectedSchema = "expectedSchema";
        final String url = "jdbc:Sqream://127.0.0.1:5000/master;user=sqream;password=sqream;cluster=false;ssl=false;service=sqream";
        Driver driver = new SQDriver();
        Properties props = new Properties();
        props.setProperty("schema", expectedSchema);
        Connection conn = driver.connect(url, props);

        String schema = conn.getSchema();

        assertEquals(expectedSchema, schema);
    }
}
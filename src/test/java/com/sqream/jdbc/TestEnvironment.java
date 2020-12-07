package com.sqream.jdbc;

import com.sqream.jdbc.logging.LoggerLevel;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.text.MessageFormat;

import static com.sqream.jdbc.logging.LoggerLevel.*;

public class TestEnvironment {

    public static final String IP = "127.0.0.1";
    public static final int PORT = 5000;
    public static final int MOCK_PORT = 6000;
    public static final String DATABASE = "master";
    public static boolean CLUSTER = false;
    public static boolean SSL = false;
    public static String USER = "sqream";
    public static String PASS = "sqream";
    public static String SERVICE = "sqream";
    public static LoggerLevel LOGGER_LEVEL = OFF;

    public static final String URL = MessageFormat.format(
            "jdbc:Sqream://{0}:{1}/{2};user={3};password={4};cluster={5};ssl={6};service={7};loggerLevel={8}",
            IP, String.valueOf(PORT), DATABASE, USER, PASS, CLUSTER, SSL, SERVICE, LOGGER_LEVEL);

    public static final String MOCK_URL = MessageFormat.format(
            "jdbc:Sqream://{0}:{1}/{2};user={3};password={4};cluster={5};ssl={6};service={7};loggerLevel={8}",
            IP, String.valueOf(MOCK_PORT), DATABASE, USER, PASS, CLUSTER, SSL, SERVICE, LOGGER_LEVEL);

    public static final String SHORT_URL = MessageFormat.format(
            "jdbc:Sqream://{0}:{1}/{2};user={3};password={4}", IP, String.valueOf(PORT), DATABASE, USER, PASS);

    public static Connection createConnection() throws SQLException {
        try {
            Class.forName("com.sqream.jdbc.SQDriver");
            return DriverManager.getConnection(URL,USER,PASS);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    public static Connection createMockConnection() throws SQLException {
        try {
            Class.forName("com.sqream.jdbc.SQDriver");
            return DriverManager.getConnection(MOCK_URL,USER,PASS);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }
}

package com.sqream.jdbc;

import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class SqreamHang2Test {

    @Test
    public void sqreamHang2Test() throws ClassNotFoundException, SQLException {

        Class.forName("com.sqream.jdbc.SQDriver");

        String url = "jdbc:Sqream://127.0.0.1:5000/master";

        try (Connection c0 = DriverManager.getConnection(url, "sqream", "sqream");
             Statement s0 = c0.createStatement()) {

            s0.execute("CREATE OR REPLACE TABLE public.class (Name VARCHAR(8),Sex VARCHAR(1),Age DOUBLE,Height DOUBLE, Weight DOUBLE);");
        }

        System.out.println ("attempting connection: " + url);
        Connection c1 = DriverManager.getConnection(url, "sqream", "sqream");
        Statement s1 = c1.createStatement();
        s1.setQueryTimeout(0);
        try {
            s1.execute("SELECT * FROM public.CLASS WHERE 0=1");
        } catch (SQLException sqle) {
            sqle.getStackTrace();
        }

        Connection c2 = DriverManager.getConnection(url, "sqream", "sqream");
        System.out.println("attempting to create statement s2");
        Statement s2 = c2.createStatement();
        s2.setQueryTimeout(0);
        System.out.println("attempting to query on s2");
        try {
            s2.execute("SELECT * FROM public.CLASS WHERE 0=1");
        } catch (SQLException sqle) {
            // sqle.printStackTrace();
        } finally {
            s2.close();
        }

        Statement s3 = c2.createStatement();
        s3.execute("CREATE OR REPLACE TABLE public.class22 (Name VARCHAR(8),Sex VARCHAR(1),Age DOUBLE,Height DOUBLE, Weight DOUBLE)");

        System.out.println ("done!");
    }
}

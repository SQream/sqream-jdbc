package com.sqream.jdbc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.sql.PreparedStatement;
import java.sql.ResultSet;


import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;


public class Perf {
    
    // Replace with your respective URL
    static final String url_src = "jdbc:Sqream://127.0.0.1:5000/master;user=sqream;password=sqream;cluster=false;ssl=false";
    static final String url_dst = "jdbc:Sqream://192.168.0.223:5000/master;user=sqream;password=sqream;cluster=false;ssl=false";

    Connection conn  = null;
    Statement stmt = null;
    ResultSet rs = null;
    DatabaseMetaData dbmeta = null;
    PreparedStatement ps = null;
    
    int res = 0;
   
    static void print(Object printable) {
        System.out.println(printable);
    }
    
    static void printbuf(ByteBuffer to_print, String description) {
        System.out.println(description + " : " + to_print);
    }
    
    static long time() {
        return System.currentTimeMillis();
    }
    
    static Date date_from_tuple(int year, int month, int day) {
        
        return Date.valueOf(LocalDate.of(year, month, day));
    }
    
    static Timestamp datetime_from_tuple(int year, int month, int day, int hour, int minutes, int seconds, int ms) {
            
        return Timestamp.valueOf(LocalDateTime.of(LocalDate.of(year, month, day), LocalTime.of(hour, minutes, seconds, ms*(int)Math.pow(10, 6))));
    }

    public void perf() throws SQLException, IOException {
        
        conn = DriverManager.getConnection(url_src,"sqream","sqream");

        // create dst table
        String sql = "create or replace table perf (bools bool, bytes tinyint, shorts smallint, ints int, bigints bigint, floats real, doubles double, strangs nvarchar(10), dates date, dts datetime)";
        stmt = conn.createStatement();
        stmt.execute(sql);
        stmt.close();
        
        // Network insert 10 million rows
        int amount = (int)Math.pow(10, 7);
        long start = time();
        sql = "insert into perf values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        ps = conn.prepareStatement(sql);
        
        for (int i=0; i < amount; i++) {
            ps.setBoolean(1, true);
            ps.setByte(1, (byte)120);
            ps.setShort(1, (short) 1400);
            ps.setInt(1, 140000);
            ps.setLong(1, (long) 5);
            ps.setFloat(1, (float)56.0);
            ps.setDouble(1, 57.0);
            ps.setDate(1, date_from_tuple(2019, 11, 26));
            ps.setTimestamp(1,  datetime_from_tuple(2019, 11, 26, 16, 45, 23, 45));
            ps.setString(1, "שוקו");
            
            ps.addBatch();
        }
        ps.executeBatch();  // Should be done automatically
        ps.close();

        print ("total network insert: " + (time() -start));
      
        
    }     
    
    public static void main(String[] args) throws SQLException, KeyManagementException, NoSuchAlgorithmException, IOException, ClassNotFoundException{
        
        // Load JDBC driver - not needed with newer version
        Class.forName("com.sqream.jdbc.SQDriver");
        
        Perf test = new Perf();   
        test.perf();
    }
}



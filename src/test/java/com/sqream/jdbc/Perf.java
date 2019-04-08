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
    static final String url_src = "jdbc:Sqream://192.168.0.74:5000/developer_regression_query;user=sqream;password=sqream;cluster=false;ssl=false";
    //static final String url_dst = "jdbc:Sqream://192.168.0.223:5000/master;user=sqream;password=sqream;cluster=false;ssl=false";
    //here sonoo is database name, root is username and password  
    
    Connection mysql_con;
    Connection conn  = null;
    Statement stmt = null;
    ResultSet rs = null;
    DatabaseMetaData dbmeta = null;
    PreparedStatement ps = null;
    String sql;
    
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
        //mysql_con=DriverManager.getConnection("jdbc:mysql://192.168.0.219:3306/perf","eliy","bladerfuK~1");  
        String sql;
        /*
        sql = "insert into perf_t2 values (?, ?)";
        ps = mysql_con.prepareStatement(sql);
        print ("before network insert");
        for(int i=1; i < 100000000; i++) {
            ps.setInt(1, 6);
            ps.setInt(2, 8);
            ps.addBatch();
            //ps.executeUpdate();m
            if (i % 10000 == 0) {
                print ("added batch number " + i);
                //ps.executeBatch();
            }
        }
        print ("after loop");
        //ps.executeBatch();  // Should be done automatically
        ps.close();
        print ("after network insert");
        //*/
        
        
        /*
        // create table
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
            ps.setByte(2, (byte)120);
            ps.setShort(3, (short) 1400);
            ps.setInt(4, 140000);
            ps.setLong(5, (long) 5);
            ps.setFloat(6, (float)56.0);
            ps.setDouble(7, 57.0);
            ps.setString(8, "bla");
            ps.setDate(9, date_from_tuple(2019, 11, 26));
            ps.setTimestamp(10, datetime_from_tuple(2019, 11, 26, 16, 45, 23, 45));
            ps.addBatch();
        }
        ps.executeBatch();  // Should be done automatically
        ps.close();

        print ("total network insert: " + (time() -start));
        
        // Check amount inserted
        sql = "select count(*) from perf";
        stmt = conn.createStatement();
        rs = stmt.executeQuery(sql);
        while(rs.next()) 
            print("row count: " + rs.getLong(1));
        rs.close();
        stmt.close();
        
        //*/
        sql = "select sum(xbigint) over (partition by xdate) from t_a";
        stmt = conn.createStatement();
        rs = stmt.executeQuery(sql);
        while(rs.next()) 
            print("item: " + rs.getLong(1));
        rs.close();
        stmt.close();
    }     
    
    public static void main(String[] args) throws SQLException, KeyManagementException, NoSuchAlgorithmException, IOException, ClassNotFoundException{
        
        // Load JDBC driver - not needed with newer version
        Class.forName("com.sqream.jdbc.SQDriver");
        //Class.forName("com.mysql.jdbc.Driver");  

        Perf test = new Perf();   
        test.perf();
    }
}



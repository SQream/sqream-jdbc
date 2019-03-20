package com.sqream.jdbc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;


import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;


public class SampleTest {
    
    // Replace with your respective URL
    static final String url_src = "jdbc:Sqream://127.0.0.1:5000/master;user=sqream;password=sqream;cluster=false;ssl=false";
    static final String url_dst = "jdbc:Sqream://192.168.0.223:5000/master;user=sqream;password=sqream;cluster=false;ssl=false";

    Connection conn_src  = null;
    Connection conn_dst  = null;
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
    
    public void testJDBC() throws SQLException, IOException {
        
        conn_src = DriverManager.getConnection(url_src,"sqream","sqream");
        conn_dst = DriverManager.getConnection(url_dst,"sqream","sqream");

        /*
        dbmeta = conn.getMetaData();
        rs = dbmeta.getTables("master", "public", "test" ,new String[] {"TABLE"} );
        while (rs.next()) {
        	
        	ResultSet rs2 = dbmeta.getColumns(null, null, "test", null);
        	while (rs2.next()) {
        		// System.out.println(rs2.getString(1)); 
        	}
        	rs2.close();
        	//System.out.format("%-15s %-10s %-20s %-10sn", rs.getString(1), rs.getString(2),  rs.getString(3), rs.getString(4)); 
    	}
        rs.close();
        conn.close();
        System.in.read();
        //*/

        //rs = dbmeta.getColumns("master", "public", "test" , null );
        //*
        String sql_src, sql_dst;
        
        /*
        // Create a table on src and generate data
        sql_src = "create or replace table test_src (ints int)";
        stmt = conn_src.createStatement();
        stmt.execute(sql_src);
        stmt.close();
        sql_dst = "create or replace table test_dst (ints int)";
        stmt = conn_dst.createStatement();
        stmt.execute(sql_dst);
        stmt.close();
        
        sql_src = "insert into test_src values (?)";
        ps = conn_src.prepareStatement(sql_src);
        for(int i=0; i < 3000000; i++) {
            ps.setInt(1, 8);
            ps.addBatch();
        }
        ps.executeBatch();  // Should be done automatically
        ps.close();
        //*/
        
        // create dst table
        sql_dst = "create or replace table test_dst (ints int)";
        stmt = conn_dst.createStatement();
        stmt.execute(sql_dst);
        stmt.close();
        
        // Stream from src to dst
        long start = time();
        sql_src = "select top 3000000 * from test_src";
        stmt = conn_src.createStatement();
        rs = stmt.executeQuery(sql_src);
        sql_dst = "insert into test_dst values (?)";
        ps = conn_dst.prepareStatement(sql_dst);
        
        while(rs.next()) {
            res = rs.getInt(1);
            ps.setInt(1, res);
            ps.addBatch();
        }
        ps.executeBatch();  // Should be done automatically
        ps.close();
        rs.close();
        stmt.close();
        print ("total network insert: " + (time() -start));
      
        // Check amount inserted
        sql_dst = "select count(*) from test_dst";
        stmt = conn_dst.createStatement();
        rs = stmt.executeQuery(sql_dst);
        while(rs.next()) 
            print("row count: " + rs.getLong(1));
        rs.close();
        stmt.close();
        
        // clean dst table
        sql_dst = "truncate table test_dst";
        stmt = conn_dst.createStatement();
        stmt.execute(sql_dst);
        stmt.close();
        
        // Copy CSV from src to disk and load to dst
        start = time();
        sql_src = "copy test_src to '/home/eliy/bla.csv'";
        stmt = conn_src.createStatement();
        stmt.execute(sql_src);
        stmt.close();
        
        sql_dst = "copy test_dst from '/home/jeremy/bla.csv'";
        stmt = conn_dst.createStatement();
        stmt.execute(sql_dst);
        stmt.close();
        print ("total csv copy: " + (time() -start));
        
        // Check amount inserted
        sql_dst = "select count(*) from test_dst";
        stmt = conn_dst.createStatement();
        rs = stmt.executeQuery(sql_dst);
        while(rs.next()) 
            print("row count: " + rs.getLong(1));
        rs.close();
        stmt.close();
    }     
    
    public static void main(String[] args) throws SQLException, KeyManagementException, NoSuchAlgorithmException, IOException, ClassNotFoundException{
        
        // Load JDBC driver - not needed with newer version
        Class.forName("com.sqream.jdbc.SQDriver");
        
        SampleTest test = new SampleTest();   
        test.testJDBC();
    }
}



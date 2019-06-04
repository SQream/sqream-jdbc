package com.sqream.jdbc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.TimeZone;
import java.sql.PreparedStatement;
import java.sql.ResultSet;


import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
//import com.sqream.connector.Connector;

public class SampleTest {
    
    // Replace with your respective URL
    

    Connection conn_src  = null;
    Connection conn_dst  = null;
    Statement stmt = null;
    ResultSet rs = null;
    DatabaseMetaData dbmeta = null;
    PreparedStatement ps = null;
    
    int res = 0;
    int res2 = 0;
    
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
        
    	String url_dst = "jdbc:Sqream://192.168.1.4:5000/master;user=sqream;password=sqream;cluster=false;ssl=false";
        String url_src = "jdbc:mysql://127.0.0.1:3306/perf";
    	conn_src = DriverManager.getConnection(url_src, "root","ssup");  
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
        long start;
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
        for(int i=0; i < 100000000; i++) {
            ps.setInt(1, 8);
            ps.addBatch();
        }
        ps.executeBatch();  // Should be done automatically
        ps.close();
        //*/
        
        //conn_src.setAutoCommit(false);
        /*
        // Stream from src to dst
        start = time();
        sql_src = "select * from perf_t limit 19000000";
        stmt = conn_src.createStatement();
        rs = stmt.executeQuery(sql_src);
        sql_dst = "insert into perf_t values (?, ?)";
        ps = conn_dst.prepareStatement(sql_dst);
        
        while(rs.next()) {
            ps.setInt(1, rs.getInt(1));
            ps.setInt(2, rs.getInt(2));
            ps.addBatch();
        }
        ps.executeBatch();  // Should be done automatically
        conn_src.commit();
        ps.close();
        rs.close();
        stmt.close();
        print ("total network insert: " + (time() -start));
      
        // Check amount inserted
        sql_dst = "select count(*) from perf_t";
        stmt = conn_dst.createStatement();
        rs = stmt.executeQuery(sql_dst);
        while(rs.next()) 
            print("row count: " + rs.getLong(1));
        rs.close();
        stmt.close();
        
        // clean dst table
        sql_dst = "truncate table perf_t";
        stmt = conn_dst.createStatement();
        stmt.execute(sql_dst);
        stmt.close();
        //*/
        
        /*
        // Stream from src to src
        start = time();
        sql_src = "select * from perf_t limit 19000000";
        stmt = conn_src.createStatement();
        rs = stmt.executeQuery(sql_src);
        sql_src = "insert into perf_t values (?, ?)";
        ps = conn_src.prepareStatement(sql_src);
        int idx = 0;
        while(rs.next()) {
            ps.setInt(1, rs.getInt(1));
            ps.setInt(2, rs.getInt(2));
            ps.addBatch();
        } 
        print ("total network insert setup: " + (time() -start));
        ps.executeBatch();  // Should be done automatically
        conn_src.commit();
        conn_src.setAutoCommit(true);
        ps.close();
        rs.close();
        stmt.close();
        print ("total network insert: " + (time() -start));
      
        // Check amount inserted
        sql_src = "select count(*) from perf_t";
        stmt = conn_src.createStatement();
        rs = stmt.executeQuery(sql_src);
        while(rs.next()) 
            print("row count: " + rs.getLong(1));
        rs.close();
        stmt.close();
        //*/
        
        
        /* // Copy CSV from src to dst
        start = time();
        sql_src = "select * from perf_t limit 19000000 into outfile '/var/lib/mysql-files/perf.csv'";
        stmt = conn_src.createStatement();
        stmt.execute(sql_src);
        stmt.close();
        print ("total csv export: " + (time() -start));
     
        start = time();
        sql_dst = "copy perf_t from '/var/lib/mysql-files/perf.csv' with delimiter '\\t'";
        stmt = conn_dst.createStatement();
        stmt.execute(sql_dst);
        stmt.close();
        print ("total csv import: " + (time() -start));
      
        // Check amount inserted
        sql_dst = "select count(*) from perf_t";
        stmt = conn_dst.createStatement();
        rs = stmt.executeQuery(sql_dst);
        while(rs.next()) 
            print("row count: " + rs.getLong(1));
        rs.close();
        stmt.close();
        
        // clean dst table
        sql_dst = "truncate table perf_t";
        stmt = conn_dst.createStatement();
        stmt.execute(sql_dst);
        stmt.close();
        //*/
        
        
       
        /*
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
        //*/
    }     
    
    public static void main(String[] args) throws SQLException, KeyManagementException, NoSuchAlgorithmException, IOException, ClassNotFoundException{
        
        // Load JDBC driver - not needed with newer version
        Class.forName("com.sqream.jdbc.SQDriver");
        //Class.forName("com.mysql.jdbc.Driver");  
        print (new Timestamp(2354345345l));
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        print (new Timestamp(2354345345l));
        //SampleTest test = new SampleTest();   
        //test.testJDBC();
    }
}



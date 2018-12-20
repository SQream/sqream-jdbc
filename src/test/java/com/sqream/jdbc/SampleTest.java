package com.sqream.jdbc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.Statement;
import java.sql.ResultSet;


import java.io.IOException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;


public class SampleTest {
    
    // Replace with your respective URL
    static final String url = "jdbc:Sqream://127.0.0.1:5000/master;user=sqream;password=sqream;cluster=false;ssl=false";
    
    Connection conn  = null;
    Statement stmt = null;
    ResultSet rs = null;
    DatabaseMetaData dbmeta = null;
    
    int res = 0;
       
    public void testJDBC() throws SQLException, IOException {
        
        
        conn = DriverManager.getConnection(url,"sqream","sqream");
        dbmeta = conn.getMetaData();
        rs = dbmeta.getTables("master", "public", "test" ,new String[] {"TABLE"} );
        //*
        while (rs.next()) {
        	
        	ResultSet rs2 = dbmeta.getColumns(null, null, "test", null);
        	while (rs2.next()) {
        		// System.out.println(rs2.getString(1)); 
        	}
        	rs2.close();
        	//System.out.format("%-15s %-10s %-20s %-10sn", rs.getString(1), rs.getString(2),  rs.getString(3), rs.getString(4)); 
    	}
    	
        //*/
        rs.close();
        conn.close();
        System.in.read();
        
        //rs = dbmeta.getColumns("master", "public", "test" , null );
        /*
        // Create a table 
        String sql = "create or replace table test (x int)";
        stmt = conn.createStatement();
        stmt.execute(sql);
        stmt.close();
        
        // Insert some valuess 
        sql = "insert into test values (5),(6)";
        stmt = conn.createStatement();
        stmt.execute(sql);
        stmt.close();

        // Retrieve
        sql = "select * from test";
        stmt = conn.createStatement();
        rs = stmt.executeQuery(sql);
        while(rs.next()) {
            res = rs.getInt(1);
            System.out.println(res);
        }
        rs.close();
        stmt.close();
    */
    }     
    
    
    public static void main(String[] args) throws SQLException, KeyManagementException, NoSuchAlgorithmException, IOException, ClassNotFoundException{
        
        // Load JDBC driver - not needed with newer version
        Class.forName("com.sqream.jdbc.SQDriver");
        
        SampleTest test = new SampleTest();   
        test.testJDBC();
    }
}



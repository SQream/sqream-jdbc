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
    
    Connection conn_src  = null;
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
    
    public void testJDBC() throws SQLException, IOException {
        
    	String url = "jdbc:Sqream://192.168.1.4:5000/developer_regression_query;user=sqream;password=sqream;cluster=false;ssl=false";
    	conn = DriverManager.getConnection(url,"sqream","sqream");
        String sql;
		
        
		sql = "select case when xint2%2=0 then xtinyint end from t_a";
        stmt = conn.createStatement();
        rs = stmt.executeQuery(sql);
        while(rs.next()) 
        	//rs.getByte(1);
            print("res: " + rs.getByte(1));
        stmt.close();
    }     
    
    public static void main(String[] args) throws SQLException, KeyManagementException, NoSuchAlgorithmException, IOException, ClassNotFoundException{
        
        Class.forName("com.sqream.jdbc.SQDriver");
        /*
        print (new Timestamp(2354345345l));
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        print (new Timestamp(2354345345l));
        //*/
        SampleTest test = new SampleTest();   
        test.testJDBC();
    }
}



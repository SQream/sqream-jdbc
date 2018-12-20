package com.sqream.jdbc;


import java.io.IOException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.MessageFormat;
import java.sql.ResultSet;
import java.sql.PreparedStatement;

import java.sql.DatabaseMetaData;    // for getTables test
import java.sql.ResultSetMetaData;


public class TestCase implements Runnable {
	

	Statement stmt = null;
	ResultSet rs = null;
	Connection conn  = null;
	PreparedStatement ps = null;
	
	// For testTables() test
	DatabaseMetaData dbmeta = null;
	ResultSetMetaData rsmeta = null;
	static final String url = "jdbc:Sqream://192.168.0.244:3108/master;user=sqream;password=sqream;cluster=true;ssl=false";
	//static final String url = "jdbc:Sqream://192.168.0.244:3108/master;user=sqream;password=sqream;cluster=true;ssl=false";

	private void cancel()
	{
		try {
			System.err.println("before STOPED");
			stmt.cancel();
			System.err.println("XXXXXXXXXXXXXXXXX");
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public TestCase()
	{
		
	}
	public TestCase(Statement _stmt)
	{
		stmt =_stmt;
	}
	
	
	public void select_case() throws SQLException {
		conn = DriverManager.getConnection(url,"sqream","sqream");
		
    	// Prepare Table
//    	System.out.println(" - Create Table t_" + table_type);
		stmt = conn.createStatement();
		
		
		
		
		//String sql = select case when xint2%2=0 then xnvarchar40 else substring(xnvarchar40,2,2) end from t_a"
		//String sql = "select case when xint2%2=0 then xtinyint end from t_a";
		String sql = "select * from big order by 1";
		//sql ="select 1 as dddd";		
		rs = stmt.executeQuery(sql);
//		int res_int;
		int res1;
		TestCase t = new TestCase(stmt);
		Thread thread = new Thread(t);
		 thread.start();
		 
		try {
		 while(rs.next()) 
		{
			//res_int = rs.getInt(1);
			res1 = rs.getInt(1);
			if(rs.wasNull()) {
				System.out.println("\\N");
			}
			else 
				System.out.println(res1);
		}
			
		
		rs.close();
		stmt.close();
		}catch(SQLException e)
		{
			System.err.println("5555555555555555555555555555555  " + e.getMessage());
			stmt.close();
		}
	}
	
	public void select_sleep() throws SQLException {
		try {
		Connection conn = DriverManager.getConnection(url,"sqream","sqream");		

		Statement stmt = conn.createStatement();
		
		String sql = "select sleep(10)";
		//sql ="select 1 as dddd";		
	    ResultSet	rs = stmt.executeQuery(sql);		
		rs.close();
		stmt.close();
		}catch(SQLException e)
		{
			System.err.println("5555555555555555555555555555555  " + e.getMessage());
			stmt.close();
		}
	}



	




	@Override
	public void run() {
		// TODO Auto-generated method stub
		try {
			Thread.currentThread().sleep(10);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		cancel();
	}

}

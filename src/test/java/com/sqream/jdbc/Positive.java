package com.sqream.jdbc;

import java.io.IOException;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.MessageFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Random;
import java.util.UUID;

import javax.script.ScriptException;

import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;


import org.junit.Test;

import com.sqream.jdbc.Connector;
import com.sqream.jdbc.Connector.ConnException;


public class Positive {
	
	//public ConnectionHandle Client =null;
    
    // Test data
    Random r = new Random();	
	boolean[] test_bools = {true, false};
	byte[] test_ubytes = {15, 0, 127};           // No unsigned byte type in java
	short[] test_shorts = {500, 0, -32768, 32767};
	int[] test_ints = {r.nextInt(), 0, -2147483648, 2147483647};
	long[] test_longs = {r.nextLong(), 0L, -9223372036854775808L, 9223372036854775807L};
	float[] test_reals = {r.nextFloat(), 0.0f, };
	double[] test_doubles = {r.nextDouble()};
	String[] test_varchars = {UUID.randomUUID().toString()};
	//String test_varchar = "koko"; 
	Date[] test_dates = {new Date(315711884629l)};
	//Timestamp[] test_datetimes = {new Timestamp(999999999999l)};	
	// Timestamp[] test_datetimes = {new Timestamp(315711884629l)};	
	Timestamp[] test_datetimes = {Timestamp.valueOf(LocalDateTime.of(LocalDate.of(2002, 9, 13), LocalTime.of(14, 56, 34, 567000000)))};	

	
	boolean   res_bool     = true;
	byte      res_ubyte    = 0;
	short     res_short    = 0;
	int       res_int      = 0;
	long      res_long     = 0l;
	float     res_real     = r.nextFloat();
	double    res_double   = 0.0;
	String    res_varchar  = "";
	String    res_nvarchar  = "";
	//String test_varchar = "koko"; 
	Date      res_date     = Date.valueOf(LocalDate.of(2012, 9, 13));
	Timestamp res_datetime = Timestamp.valueOf(LocalDateTime.of(LocalDate.of(2002, 9, 13), LocalTime.of(14, 56, 34, 567)));
	///*
	boolean test_bool = false;
	byte test_ubyte = 15;
	short test_short = 500;
	int test_int = r.nextInt();
	long test_long = r.nextLong();
	float test_real = r.nextFloat();
	double test_double = r.nextDouble();
	String test_varchar = UUID.randomUUID().toString();
	String test_nvarchar = "שוקו";
	//String test_varchar = "koko"; 
	Date test_date = Date.valueOf(LocalDate.of(2002, 9, 13));
	Timestamp test_datetime = Timestamp.valueOf(LocalDateTime.of(LocalDate.of(2002, 9, 13), LocalTime.of(14, 56, 34, 567)));
    
	static void print(Object printable) {
		System.out.println(printable);
	}
	
	static long time() {
		return System.currentTimeMillis();
	}
	
	public boolean test_varchar() throws IOException, ScriptException, ConnException, NoSuchAlgorithmException, KeyManagementException  {
	    /* Test that get_varchar returns corect results for all types */
		
		boolean a_ok = false;
		Connector conn = new Connector("127.0.0.1", 5000, false, false);
		conn.connect("master", "sqream", "sqream", "sqream");
		
		// Prepare Table
		String sql = "create or replace table mcVarc (t_bool bool, t_ubyte tinyint, t_short smallint, t_int int, t_long bigint, t_float real, t_double double, t_date date, t_datetime datetime, t_varchar varchar(10), t_nvarchar nvarchar(10))";
		conn.execute(sql);		
		conn.close();
		
		// Insert data
		sql = "insert into mcVarc values (false, 14, 140, 1400, 14000000, 14.1, 14.12345, '2013-11-23', '2013-11-23 14:56:47.1', 'wuzz', 'up')";
		conn.execute(sql);		
		conn.close();
		
		// Retrieve and compare
		sql = "select * from mcVarc";
		conn.execute(sql);		
		
		while(conn.next()) { 
			String res = conn.get_nvarchar(11);
			if (conn.get_boolean(1) != false)  
				System.out.println("Wrong return value on getBoolen");
			else if (conn.get_ubyte(2) != 14) 
				System.out.println("Wrong return value on get_ubyte");
			else if (conn.get_short(3) != 140)
				System.out.println("Wrong return value on get_short");
			else if (conn.get_int(4) != 1400) 
				System.out.println("Wrong return value on get_int");
			else if (conn.get_long(5) != 14000000l) 
				System.out.println("Wrong return value on get_long");
			else if (conn.get_float(6) != 14.1f) 
				System.out.println("Wrong return value on get_float");
			else if (conn.get_double(7) != 14.12345) 
				System.out.println("Wrong return value on get_double");
			else if (!conn.get_date(8).toString().equals("2013-11-23")) 
				System.out.println("Wrong return value on get_date");
			else if (!conn.get_datetime(9).toString().equals("2013-11-23 14:56:47.1"))
				System.out.println("Wrong return value on get_datetime");
			else if (!conn.get_varchar(10).trim().equals("wuzz")) 
				System.out.println("Wrong return value on get_varchar");
			
			else if (!res.equals("up"))
				System.out.println("Wrong return value on get_nvarchar");
			else
				System.out.println("get_varchar test ok");
				a_ok = true;
		}
		conn.close();
		// System.out.println(a_ok);
		return a_ok;
	}
	
	public boolean insert(String table_type)throws IOException, ScriptException, ConnException, NoSuchAlgorithmException, KeyManagementException  {
    	
    	boolean a_ok = false;
    	String table_name = table_type.contains("varchar(") ?  table_type.substring(0,7) : table_type;
    	Connector conn = new Connector("127.0.0.1", 5000, false, false);
    	conn.connect("master", "sqream", "sqream", "sqream");
		
    	// Prepare Table
//    	System.out.println(" - Create Table t_" + table_type);
    	String sql = MessageFormat.format("create or replace table t_{0} (x {1})", table_name, table_type);
		conn.execute(sql);		
		
		conn.close();
		
		// Insert value
//		System.out.println(" - Insert test value " + table_type);
		if (table_type == "bool") 
			for (boolean test : test_bools) {
				test_bool = test;
				sql = MessageFormat.format("insert into t_{0} values (?)", table_name);
				conn.execute(sql);
				 
				conn.set_boolean(1, test_bool); 
				send_and_retreive_result (conn, table_name, table_type);
				a_ok = is_identical(table_type);}
		else if (table_type == "tinyint") 
			for (byte test : test_ubytes) {
				test_ubyte = test;
				sql = MessageFormat.format("insert into t_{0} values (?)", table_name);
				conn.execute(sql);
				 
				conn.set_ubyte(1, test_ubyte);
				send_and_retreive_result (conn, table_name, table_type); 
				a_ok = is_identical(table_type);}
		else if (table_type == "smallint") 
			for (short test : test_shorts) {
				test_short = test;
				sql = MessageFormat.format("insert into t_{0} values (?)", table_name);
				conn.execute(sql);
				
				conn.set_short(1, test_short);
				send_and_retreive_result (conn, table_name, table_type);
				a_ok = is_identical(table_type);}
		else if (table_type == "int") 
			for (int test : test_ints) {
				test_int = test;
				sql = MessageFormat.format("insert into t_{0} values (?)", table_name);
				conn.execute(sql);
				conn.set_int(1, test_int);
				send_and_retreive_result (conn, table_name, table_type); 
				a_ok = is_identical(table_type);}
		else if (table_type == "bigint")
			for (long test : test_longs) {
				test_long = test;
				sql = MessageFormat.format("insert into t_{0} values (?)", table_name);
				conn.execute(sql);
				 
				conn.set_long(1, test_long);
				send_and_retreive_result (conn, table_name, table_type); 
				a_ok = is_identical(table_type);}
		else if (table_type == "real")
			for (float test : test_reals) {
				test_real = test;
				sql = MessageFormat.format("insert into t_{0} values (?)", table_name);
				conn.execute(sql);
				
				conn.set_float(1, test_real);
				send_and_retreive_result (conn, table_name, table_type);
				a_ok = is_identical(table_type);}
		else if (table_type == "double")
			for (double test : test_doubles) {
				test_double = test;
				sql = MessageFormat.format("insert into t_{0} values (?)", table_name);
				conn.execute(sql);
				
				conn.set_double(1, test_double);
				send_and_retreive_result (conn, table_name, table_type); 
				a_ok = is_identical(table_type);}
		else if (table_type == "varchar(100)")
			for (String test : test_varchars) {
				test_varchar = test;
				sql = MessageFormat.format("insert into t_{0} values (?)", table_name);
				conn.execute(sql);
				
				conn.set_varchar(1, test_varchar);
				send_and_retreive_result (conn, table_name, table_type); 
				a_ok = is_identical(table_type);}
		else if (table_type == "nvarchar(4)")
			for (String test : test_varchars) {
				test_varchar = test;
				sql = MessageFormat.format("insert into t_{0} values (?)", table_name);
				conn.execute(sql);
				 
				conn.set_nvarchar(1, test_nvarchar);
				send_and_retreive_result (conn, table_name, table_type); 
				a_ok = is_identical(table_type);}
		else if (table_type == "date")
			for (Date test : test_dates) {
				test_date = test;
				sql = MessageFormat.format("insert into t_{0} values (?)", table_name);
				conn.execute(sql);
				 
				conn.set_date(1, test_date);
				send_and_retreive_result (conn, table_name, table_type); 
				a_ok = is_identical(table_type);}
		else if (table_type == "datetime")
			for (Timestamp test : test_datetimes) {
				test_datetime = test;
				//System.out.println("datetime: " + test_datetime);
				sql = MessageFormat.format("insert into t_{0} values (?)", table_name);
				conn.execute(sql);
				
				conn.set_datetime(1, test_datetime);
				send_and_retreive_result (conn, table_name, table_type); 
				a_ok = is_identical(table_type);}
	
		return a_ok;
    }
    
    public void send_and_retreive_result (Connector conn, String table_name, String table_type) throws ConnException, IOException, ScriptException, NoSuchAlgorithmException {
		
    	conn.next();
		conn.close();
		//*
		// Retreive
		// System.out.println(" - Getting " + table_type + " value back for value");
		String sql = MessageFormat.format("select * from t_{0}", table_name);
		conn.execute(sql);		
		
		//System.out.println("type_out: " + type_out);
		
		// int res = conn.get_int(1);
		//*
		while(conn.next())
		{
			if (table_type == "bool") 
				res_bool = conn.get_boolean(1);
			else if (table_type == "tinyint") 
				res_ubyte = conn.get_ubyte(1);
			else if (table_type == "smallint") 
				res_short = conn.get_short(1);
			else if (table_type == "int") 
				res_int = conn.get_int(1);
			else if (table_type == "bigint")
				res_long = conn.get_long(1);
			else if (table_type == "real")
				res_real = conn.get_float(1);
			else if (table_type == "double")
				res_double = conn.get_double(1);
			else if (table_type == "varchar(100)")
				res_varchar = conn.get_varchar(1);
			else if (table_type == "nvarchar(4)")
				res_nvarchar = conn.get_nvarchar(1);
			else if (table_type == "date")
				res_date = conn.get_date(1);
			else if (table_type == "datetime")
				res_datetime = conn.get_datetime(1); 
		}  //*/	
		conn.close();
	}
    
	public boolean is_identical(String table_type) {
		
		boolean res = false;
		//assertEquals(test_int, res_int);
		
		if (table_type == "bool" && test_bool != res_bool)
			System.out.println("Results not identical on table type " + table_type + " " + test_bool + " " + res_bool);
		else if (table_type == "tinyint" && test_ubyte != res_ubyte) 
			System.out.println("Results not identical on table type " + table_type + " " + test_ubyte + " " + res_ubyte);		
		else if (table_type == "smallint" && test_short != res_short) 
			System.out.println("Results not identical on table type " + table_type + " " + test_short + " " + res_short);		
		else if (table_type == "int" && test_int != res_int) 
			System.out.println("Results not identical on table type " + table_type + " " + test_int + " " + res_int);		
		else if (table_type == "bigint" && test_long != res_long) 
			System.out.println("Results not identical on table type " + table_type + " " + test_long + " " + res_long);		
		else if (table_type == "real" && test_real != res_real) 
			System.out.println("Results not identical on table type " + table_type + " " + test_real + " " + res_real);		
		else if (table_type == "double" && test_double != res_double) 
			System.out.println("Results not identical on table type " + table_type + " " + test_double + " " + res_double);		
		else if (table_type == "varchar(100)" && !test_varchar.equals(res_varchar.trim()))  {
			System.out.println("Results not identical on table type " + table_type + " " + test_varchar + " " + res_varchar);		
			System.out.println(test_varchar.compareTo(res_varchar) + "a"+ test_varchar.length() + "b" + res_varchar.length());}
		else if (table_type == "nvarchar(4)" && !test_nvarchar.equals(res_nvarchar.trim()))  {
			System.out.println("Results not identical on table type " + table_type + " " + test_varchar + " " + res_varchar);		
			System.out.println(test_varchar.compareTo(res_nvarchar) + "a"+ test_nvarchar.length() + "b" + res_varchar.length());}
		else if (table_type == "date" && Math.abs(test_date.compareTo(res_date)) > 1) {
		//else if (table_type == "date" && !test_date.equals(res_date))  {  
			System.out.println("Results not identical on table type " + table_type + " " + test_date + " " + test_date.getTime() + " " + res_date + " " + res_date.getTime());		
			System.out.println(test_date.compareTo(res_date));}
		else if (table_type == "datetime" && !test_datetime.equals(res_datetime))
		//else if (table_type == "datetime" && Math.abs(test_datetime.compareTo(res_datetime)) > 1) 
			System.out.println("Results not identical on table type " + table_type + " " + test_datetime + " " + test_datetime.getTime() + " " + res_datetime + " " + res_datetime.getTime());		
		
		else {
			System.out.println(" Results identical");
			res = true;}
	
	return res;  	
		
	}
	
	
    public boolean autoflush(int total_inserts, int insert_every) throws IOException, ScriptException, ConnException, NoSuchAlgorithmException, KeyManagementException   {
    
    	boolean a_ok = false;
    	Connector conn = new Connector("127.0.0.1", 5000, false, false);
		conn.connect("master", "sqream", "sqream", "sqream");
    	
    	// Prepare Table
    	String table_type = "int";
    	
    	//int row_num = 100000000;
    	//System.out.println(" - Create Table t_" + table_type);
    	String sql = MessageFormat.format("create or replace table t_{0} (x {0})", table_type);
    	sql = "create or replace table test (x int, y nvarchar(50))";
		conn.execute(sql);		
		conn.close();
    	
		//System.out.println(" - Insert " + table_type + " " + total_inserts + " times");
		sql = MessageFormat.format("insert into test values (?, ?)", table_type);
		conn.execute(sql);
		 
		int multi_row_value = 8;
		String multi_row_string = "koko";
    	//* Insert a bunch of rows
		for(int i =0 ; i< total_inserts; i++) {   
          conn.set_int(1,  multi_row_value);
          conn.set_nvarchar(2,  multi_row_string);
          conn.next();  //  if ((i)%commitEvery==0)  
    
		}          
		conn.close();
		print("Autoflush ok");
		
    	return a_ok;
    }
    
    @Test
    public void insertBool() throws  IOException, SQLException, ScriptException, ConnException, NoSuchAlgorithmException, KeyManagementException{
    	new Positive().insert("bool");
    }
    @Test
    public void insertTinyint() throws  IOException, SQLException, ScriptException, ConnException, NoSuchAlgorithmException, KeyManagementException{
    	new Positive().insert("tinyint");
    }
    @Test
    public void insertSmallint() throws  IOException, SQLException, ScriptException, ConnException, NoSuchAlgorithmException, KeyManagementException{
    	new Positive().insert("smallint");
    }
    @Test
    public void insertInt() throws IOException, SQLException, ScriptException, ConnException, NoSuchAlgorithmException, KeyManagementException{
    	new Positive().insert("int");
    }
    @Test
    public void insertBigint() throws  IOException, SQLException, ScriptException, ConnException, NoSuchAlgorithmException, KeyManagementException{
    	new Positive().insert("bigint");
    }
    @Test
    public void insertReal() throws  IOException, SQLException, ScriptException, ConnException, NoSuchAlgorithmException, KeyManagementException{
    	new Positive().insert("real");
    }
    @Test
    public void insertDouble() throws  IOException, SQLException, ScriptException, ConnException, NoSuchAlgorithmException, KeyManagementException{
    	new Positive().insert("double");
    }
    @Test
    public void insertDatetime() throws  IOException, SQLException, ScriptException, ConnException, NoSuchAlgorithmException, KeyManagementException{
    	new Positive().insert("datetime");
    }
    @Test
    public void insertDate() throws  IOException, SQLException, ScriptException, ConnException, NoSuchAlgorithmException, KeyManagementException{
    	new Positive().insert("date");
    }
    @Test
    public void insertVarchar100() throws  IOException, SQLException, ScriptException, ConnException, NoSuchAlgorithmException, KeyManagementException{
    	new Positive().insert("varchar(100)");
    }
    @Test
    public void insertnVarchar100() throws IOException, SQLException, ScriptException, ConnException, NoSuchAlgorithmException, KeyManagementException{
    	new Positive().insert("nvarchar(100)");
    }
    //*/
     @Test
     public void autoFlush() throws  IOException, SQLException, ScriptException, ConnException, NoSuchAlgorithmException, KeyManagementException{
    	 new Positive().autoflush(10000, 100);
     }
     
    public static void main(String[] args)  throws IOException, ScriptException, ConnException, NoSuchAlgorithmException, KeyManagementException {
    	
    	//*
    	Positive pos_tests = new Positive();
		 String[] typelist = {"bool", "tinyint", "smallint", "int", "bigint", "real", "double", "varchar(100)", "nvarchar(4)", "date", "datetime"};
		
		if (!pos_tests.test_varchar())
			throw new java.lang.RuntimeException("get_varchar test failed");
		 //*
		for (String col_type : typelist)
			if(!pos_tests.insert(col_type))  
				throw new java.lang.RuntimeException("Not all type checks returned identical");
		//*
		try {
			pos_tests.autoflush(1000000, 50000);
		}catch (java.lang.ArrayIndexOutOfBoundsException e) {
			System.out.println("Correct error on overflowing buffer with addBatch()");
		}   //*/ 
    	
		/*
		Connector conn = new Connector("127.0.0.1", 3108, true, false);
		conn.connect("master", "sqream", "sqream", "sqream");
		
		String stmt = "select count (*) from shoko";
		conn.execute(stmt);
		while(conn.next()) 
			print("row count: " + conn.get_long(1));
		conn.close();
		
		long start = time();
		stmt = "select top 3 * from shoko";
		conn.execute(stmt);
		while(conn.next()) {
			print("boolean received: " + conn.get_boolean(1));
			print("byte received: " + conn.get_ubyte(2));
			print("short received: " + conn.get_short(3));
			print("int received: " + conn.get_int(4));
			print("long received: " + conn.get_long(5));
			print("float received: " + conn.get_float(6));
			print("double received: " + conn.get_double(7));
			print("nvarchar received: " + conn.get_nvarchar(8));
			print("date received: " + conn.get_date(9));
			print("datetime received: " + conn.get_datetime(10));
			//print("varchar received: " + perf.get_int(1));
		}
		conn.close();
		print("Select total: " + (time() - start));
		*/
    }  
}



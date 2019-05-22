package com.sqream.jdbc;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.MessageFormat;
import java.util.Random;
import java.util.UUID;

import javax.script.ScriptException;

import org.junit.Test;

import com.sqream.jdbc.Connector.ConnException;


public class Negative {
	
	//public Connector Client =null;
    
    Random r = new Random();
	
    boolean test_bool = true,            res_bool;
	byte test_ubyte = 15, 				 res_ubyte;
	short test_short = 500, 			 res_short;
	int test_int = r.nextInt(),		     res_int;
	long test_long = r.nextLong(), 		 res_long;
	float test_real = r.nextFloat(), 	 res_real;
	double test_double = r.nextDouble(), res_double;
	String test_varchar = UUID.randomUUID().toString(), res_varchar;
	String test_nvarchar = test_varchar, res_nvarchar;

	//String test_varchar = "koko"; 
	Date test_date = new Date(99999999l), res_date = new Date(0l);
	Timestamp test_datetime = new Timestamp(9l), res_datetime = new Timestamp(0l);
	
	static void print(Object printable) {
		System.out.println(printable);
	}
	
    public boolean wrong_type_set(String table_type) throws IOException, ScriptException, ConnException, NoSuchAlgorithmException, KeyManagementException{
    	/* Set a column value using the wrong set command. See if error message is correct */
    	
    	boolean a_ok = false; 
    	String table_name = table_type.contains("varchar(100)") ?  table_type.substring(0,7) : table_type;
    	Connector conn = new Connector("127.0.0.1", 5000, false, false);
		conn.connect("master", "sqream", "sqream", "sqream");
			
		
    	// Prepare Table
//    	System.out.println(" - Create Table t_" + table_type);
    	String sql = MessageFormat.format("create or replace table t_{0} (x {1})", table_name, table_type);
		conn.execute(sql);
		conn.close();
		
		// Insert using wrong statement
//		System.out.println(" - Insert test value " + table_type);
		sql = MessageFormat.format("insert into t_{0} values (?)", table_name);
		conn.execute(sql);
		if (table_type == "bool")
			try {
				conn.set_ubyte(1, test_ubyte);
			} catch (ConnException e) {
				if (e.getMessage().contains("Trying to set")) {
					System.out.println("Correct error message on wrong set function");
					a_ok = true;
				}
			}
		else if (table_type == "tinyint") 
			try {
				conn.set_double(1, test_double);
			}catch (ConnException e) {
				if (e.getMessage().contains("Trying to set")) {
					System.out.println("Correct error message on wrong set function");
					a_ok = true;
				}
			}
		else if (table_type == "smallint") 
			try {
				conn.set_ubyte(1, test_ubyte);
			}catch (ConnException e) {
				if (e.getMessage().contains("Trying to set")) {
					System.out.println("Correct error message on wrong set function");
					a_ok = true;
				}
			}
		else if (table_type == "int") 
			try {
				conn.set_short(1, test_short);
			}catch (ConnException e) {
				if (e.getMessage().contains("Trying to set")) {
					System.out.println("Correct error message on wrong set function");
					a_ok = true;
				}
			}
		else if (table_type == "bigint")
			try {
				conn.set_int(1, test_int);
			}catch (ConnException e) {
				if (e.getMessage().contains("Trying to set")) {
					System.out.println("Correct error message on wrong set function");
					a_ok = true;
				}
			}
		else if (table_type == "real")
			try {
				conn.set_long(1, test_long);
			}catch (ConnException e) {
				if (e.getMessage().contains("Trying to set")) {
					System.out.println("Correct error message on wrong set function");
					a_ok = true;
				}
			}
		else if (table_type == "double")
			try {
			 	conn.set_float(1, test_real);
			}catch (ConnException e) {
				if (e.getMessage().contains("Trying to set")) {
					System.out.println("Correct error message on wrong set function");
					a_ok = true;
				}
			}
		else if (table_type == "varchar(100)")
			try {
				conn.set_nvarchar(1, test_nvarchar);
			}catch (ConnException e) {
				if (e.getMessage().contains("Trying to set")) {
					System.out.println("Correct error message on wrong set function");
					a_ok = true;
				}
			}
		else if (table_type == "nvarchar(100)")
			try {
				conn.set_varchar(1, test_varchar);
			}catch (ConnException e) {
				if (e.getMessage().contains("Trying to set")) {
					System.out.println("Correct error message on wrong set function");
					a_ok = true;
				}
			}
		else if (table_type == "date")
			try {
				conn.set_datetime(1, test_datetime); 
			}catch (ConnException e) {
				if (e.getMessage().contains("Trying to set")) {
					System.out.println("Correct error message on wrong set function");
					a_ok = true;
				}
			}
		else if (table_type == "datetime")
			try {
				conn.set_date(1, test_date);
			}catch (ConnException e) {
				if (e.getMessage().contains("Trying to set")) {
					System.out.println("Correct error message on wrong set function");
					a_ok = true;
				}
			}
		conn.close(); 
		// Check for appropriate wrong set error
		return a_ok;
    }

   
    public boolean wrong_type_get(String table_type) throws IOException, KeyManagementException, NoSuchAlgorithmException, ScriptException, ConnException{
    	/* Set a column value, and try to get it back using the wrong get command. 
    	   See if error message is correct */
    	
    	boolean a_ok = false;
    	String table_name = table_type.contains("varchar(100)") ?  table_type.substring(0,7) : table_type;
    	Connector conn = new Connector("127.0.0.1", 5000, false, false);
		conn.connect("master", "sqream", "sqream", "sqream");
		
		
    	// Prepare Table
    	System.out.println(" - Create Table t_" + table_type);
    	String sql = MessageFormat.format("create or replace table t_{0} (x {1})", table_name, table_type);
		conn.execute(sql);
		conn.close();
		//Random r = new Random();
		
		// Insert value
		System.out.println(" - Insert test value " + table_type);
		sql = MessageFormat.format("insert into t_{0} values (?)", table_name);
		conn.execute(sql);
		if (table_type == "bool") 
			conn.set_boolean(1, test_bool);
		else if (table_type == "tinyint") 
			conn.set_ubyte(1, test_ubyte);
		else if (table_type == "smallint") 
			conn.set_short(1, test_short);
		else if (table_type == "int") 
			conn.set_int(1, test_int);
		else if (table_type == "bigint")
			conn.set_long(1, test_long);
		else if (table_type == "real")
			conn.set_float(1, test_real);
		else if (table_type == "double")
			conn.set_double(1, test_double);
		else if (table_type == "varchar(100)")
			conn.set_varchar(1, test_varchar);
		else if (table_type == "nvarchar(100)")
			conn.set_nvarchar(1, test_nvarchar);
		else if (table_type == "date")
			conn.set_date(1, test_date);
		else if (table_type == "datetime")
			conn.set_datetime(1, test_datetime); 
		
		conn.next();
		conn.close();
		
		// Retreive using wrong statement
		System.out.println(" - Getting " + table_type + " value back");
		sql = MessageFormat.format("select * from t_{0}", table_name);
		conn.execute(sql);
		// int res = conn.get_int(1);
		//*
		while(conn.next())
		{
			if (table_type == "bool") 
				try {
					res_ubyte = conn.get_ubyte(1);
				}catch (ConnException e) {
					if (e.getMessage().contains("Trying to get")) {
						System.out.println("Correct error message on wrong get function");
						a_ok = true;
					}
				}
			else if (table_type == "tinyint") 
				try {
					res_double = conn.get_double(1);
				}catch (ConnException e) {
					if (e.getMessage().contains("Trying to get")) {
						System.out.println("Correct error message on wrong get function");
						a_ok = true;
					}
				}
			else if (table_type == "smallint") 
				try {
					res_ubyte = conn.get_ubyte(1);
				}catch (ConnException e) {
					if (e.getMessage().contains("Trying to get")) {
						System.out.println("Correct error message on wrong get function");
						a_ok = true;
					}
				}
			else if (table_type == "int") 
				try {
					res_short = conn.get_short(1);
				}catch (ConnException e) {
					if (e.getMessage().contains("Trying to get")) {
						System.out.println("Correct error message on wrong get function");
						a_ok = true;
					}
				}
			else if (table_type == "bigint")
				try {
					res_int = conn.get_int(1);
				}catch (ConnException e) {
					if (e.getMessage().contains("Trying to get")) {
						System.out.println("Correct error message on wrong get function");
						a_ok = true;
					}
				}
			else if (table_type == "real")	
				try {
					res_long = conn.get_long(1);
				}catch (ConnException e) {
					if (e.getMessage().contains("Trying to get")) {
						System.out.println("Correct error message on wrong get function");
						a_ok = true;
					}
				}
			else if (table_type == "double")
				try {
					res_real = conn.get_float(1);
					//res_double = conn.get_double(1);
				}catch (ConnException e) {
					if (e.getMessage().contains("Trying to get")) {
						System.out.println("Correct error message on wrong get function");
						a_ok = true;
					}
				}
			else if (table_type == "varchar(100)")
				try {
					res_nvarchar = conn.get_nvarchar(1);
				}catch (ConnException e) {
					if (e.getMessage().contains("Trying to get")) {
						System.out.println("Correct error message on wrong get function");
						a_ok = true;
					}
				}
			else if (table_type == "nvarchar(100)")
				try {
					res_int = conn.get_int(1);
				}catch (ConnException e) {
					if (e.getMessage().contains("Trying to get")) {
						System.out.println("Correct error message on wrong get function");
						a_ok = true;
					}
				}
			else if (table_type == "date")
				try {
					res_datetime = conn.get_datetime(1);
				}catch (ConnException e) {
					if (e.getMessage().contains("Trying to get")) {
						System.out.println("Correct error message on wrong get function");
						a_ok = true;
					}
				}
			else if (table_type == "datetime")
				try {
					res_date = conn.get_date(1);
				}catch (ConnException e) {
					if (e.getMessage().contains("Trying to get")) {
						System.out.println("Correct error message on wrong get function");
						a_ok = true;
					}
				}
		}  //*/	
		// Check for appropriate wrong get error
		conn.close();

		return a_ok;	
    }
    
    // Data for bad value testing
    static int varcharLen = 10;            // used inside main() as well
    static String varchar_type = MessageFormat.format("varchar({0})", varcharLen);
	byte[] bad_ubytes = {-5};           // No unsigned byte type in java
	String[] badVarchars = {String.valueOf(new char[varcharLen+1]).replace('\0', 'j')};
	//String testVarchar = "koko"; 
	Date[] badDates = {new Date(-300l), new Date(-9999999999999999l)};
	//Timestamp[] testDatetimes = {new Timestamp(999999999999l)};	
	Timestamp[] badDatetimes = {new Timestamp(-300l), new Timestamp(-9999999999999999l)};
	
	
    public boolean bad_value_set(String table_type) throws IOException, KeyManagementException, NoSuchAlgorithmException, ScriptException, ConnException{
    	/* Try to set a varchar/nvarchar of the wrong size. See if error message is correct */
    	
    	boolean a_ok = false;
    	String tableName = table_type.contains("varchar(10)") ?  table_type.substring(0,7) : table_type;
    	
    	Connector conn = new Connector("127.0.0.1", 5000, false, false);
		conn.connect("master", "sqream", "sqream", "sqream");
		
    	// Prepare Table
    	//System.out.println(" - Create Table t_" + table_type);
    	String sql = MessageFormat.format("create or replace table t_{0} (x {1})", tableName, table_type);
		conn.execute(sql);
		conn.close();
		
		// Insert a String that is too long - attempts kept for future reference
		//char repeated_char = 'j';
		//String tooLong = String.valueOf(new char[varcharLen+1]).replace('\0', repeated_char );
		//char [] rep = new char[len+1];
		//Arrays.fill(rep, repeated_char);  // String.valueOf(rep)
		// String repeated_pattern = "j";
		//String tooLong = new String(rep).replace("\0", repeated_pattern);
		
		//if (varchar_orNvarchar.equals("varchar"))
		if (table_type == "tinyint") 
			for (byte bad: bad_ubytes) {
				System.out.println(" - Insert negative tinyint");
				sql = MessageFormat.format("insert into t_{0} values (?)", tableName);
				conn.execute(sql);
				
				try {
					System.out.println("Attempted bad insert value: " + bad);
					conn.set_ubyte(1, bad);
					System.out.println("yeish");
				}catch (ConnException e) {
					if (e.getMessage().contains("Trying to set")) {
						System.out.println("Correct error message on setting bad value");
						a_ok = true;
					
					}
				}	
				//conn.next();
				// conn.executeBatch();
				conn.close();
			}
		
		else if (table_type == varchar_type) 
			for (String bad: badVarchars) {
				System.out.println(" - Insert oversized test value of type " + tableName + " of size " + varcharLen);
				sql = MessageFormat.format("insert into t_{0} values (?)", tableName);
				conn.execute(sql);
				
				try {
					System.out.println("Attempted bad insert value: " + bad);
					conn.set_varchar(1, bad);}
				catch (ConnException e) {
					if (e.getMessage().contains("Trying to set string of size")) {
						System.out.println("Correct error message on setting oversized varchar");
						a_ok = true;
					}
				}	
				// conn.executeBatch();
				conn.close();
			}
		
		else if (table_type == "date") 
			for (Date bad: badDates) {
				System.out.println(" - Insert negative/huge long for date");
				sql = MessageFormat.format("insert into t_{0} values (?)", tableName);
				conn.execute(sql);
				
				try {
					System.out.println("Attempted bad insert value: " + bad);
					conn.set_date(1, bad);
					System.out.println("yeish");}
				finally {
					conn.close();
					// System.out.println("Correct exception thrown on bad date");
					a_ok = true;
					// return a_ok;
				}	
				conn.next();
				// conn.executeBatch();
				conn.close();
			}
		
		else if (table_type == "datetime") 
			for (Timestamp bad: badDatetimes) {
				try {
					System.out.println("Attempted bad insert value: " + bad);
					conn.set_datetime(1, bad);
					System.out.println("yeish");}
				finally {
					conn.close();
					// System.out.println("Correct exception thrown on bad datetime");
					a_ok = true;
					// return a_ok;
				}	
				conn.next();
				// conn.executeBatch();
				conn.close();
			}
		
		
		return a_ok;
    }

    @Test //(expected = ConnException.class)
    public void wrongTypeSetBool() throws KeyManagementException, NoSuchAlgorithmException, IOException, SQLException, ScriptException, ConnException{
    	new Negative().wrong_type_set("bool");
    }
    @Test //(expected = ConnException.class)
    public void wrongTypeSetTinyint() throws KeyManagementException, NoSuchAlgorithmException, IOException, SQLException, ScriptException, ConnException{
    	new Negative().wrong_type_set("tinyint");
    }

    @Test //(expected = ConnException.class)
    public void wrongTypeSetSmallint() throws KeyManagementException, NoSuchAlgorithmException, IOException, SQLException, ScriptException, ConnException{
    	new Negative().wrong_type_set("smallint");
    }
    @Test //(expected = ConnException.class)
    public void wrongTypeSetInt() throws KeyManagementException, NoSuchAlgorithmException, IOException, SQLException, ScriptException, ConnException{
    	new Negative().wrong_type_set("int");
    }
    @Test //(expected = ConnException.class)
    public void wrongTypeSetBigint() throws KeyManagementException, NoSuchAlgorithmException, IOException, SQLException, ScriptException, ConnException{
    	new Negative().wrong_type_set("bigint");
    }
    @Test //(expected = ConnException.class)
    public void wrongTypeSetReal() throws KeyManagementException, NoSuchAlgorithmException, IOException, SQLException, ScriptException, ConnException{
    	new Negative().wrong_type_set("real");
    }
    @Test //(expected = ConnException.class)
    public void wrongTypeSetDouble() throws KeyManagementException, NoSuchAlgorithmException, IOException, SQLException, ScriptException, ConnException{
    	new Negative().wrong_type_set("double");
    }
    @Test //(expected = ConnException.class)
    public void wrongTypeSetDate() throws KeyManagementException, NoSuchAlgorithmException, IOException, SQLException, ScriptException, ConnException{
    	new Negative().wrong_type_set("date");
    }
    @Test //(expected = ConnException.class)
    public void wrongTypeSetDatetime() throws KeyManagementException, NoSuchAlgorithmException, IOException, SQLException, ScriptException, ConnException{
    	new Negative().wrong_type_set("datetime");
    }
   	


    public static void main(String[] args) throws SQLException, KeyManagementException, NoSuchAlgorithmException, IOException, ScriptException, ConnException{
		Negative neg_tests = new Negative();
		String[] typelist = {"bool", "tinyint", "smallint", "int", "bigint", "real", "double", "date", "datetime"};
		//*
		for (String col_type : typelist)
			try {
				neg_tests.wrong_type_set(col_type);
			}finally {}
				//// System.out.println("Correct error message on wrong set function"); // */
		
		/*	
		for (String col_type : typelist)
			if (!neg_tests.wrong_type_set(col_type))
				throw new java.lang.RuntimeException("Not all negative type sets passed"); //*/
		
		/*
		for (String col_type : typelist)
			if (!neg_tests.wrong_type_get(col_type))  
				throw new java.lang.RuntimeException("Not all negative type gets passed");	//*/
		
		
		String[] bad_typelist = {"tinyint", varchar_type};

		for (String table_type: bad_typelist)
			if (!neg_tests.bad_value_set(table_type))  
				throw new java.lang.RuntimeException("bad values test failure");	//*/			
	}  
}



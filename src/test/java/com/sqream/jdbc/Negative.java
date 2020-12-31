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
import java.util.logging.Logger;

import javax.script.ScriptException;

import com.sqream.jdbc.connector.ConnectorImpl;
import org.junit.Test;

import com.sqream.jdbc.connector.ConnException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static com.sqream.jdbc.TestEnvironment.*;
import static org.junit.Assert.*;

@RunWith(JUnit4.class)
public class Negative {

	private static final Logger log = Logger.getLogger(Negative.class.toString());

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

    private void wrong_type_set(String table_type) throws IOException, ScriptException, ConnException, NoSuchAlgorithmException, KeyManagementException{
    	/* Set a column value using the wrong set command. See if error message is correct */

    	String table_name = table_type.contains("varchar(100)") ?  table_type.substring(0,7) : table_type;
    	ConnectorImpl conn = new ConnectorImpl(
    			ConnectionParams.builder()
						.ipAddress(IP)
						.port(String.valueOf(PORT))
						.cluster(String.valueOf(CLUSTER))
						.useSsl(String.valueOf(SSL))
						.build());
		conn.connect(DATABASE, USER, PASS, SERVICE);

    	// Prepare Table
    	String sql = MessageFormat.format("create or replace table t_{0} (x {1})", table_name, table_type);
		conn.execute(sql);
		conn.close();

		// Insert using wrong statement
		sql = MessageFormat.format("insert into t_{0} values (?)", table_name);
		conn.execute(sql);
		if ("bool".equals(table_type))
			try {
				conn.setUbyte(1, test_ubyte);
			} catch (UnsupportedOperationException e) {
				assertTrue(e.getMessage().contains("Trying to set"));
			}
		else if ("tinyint".equals(table_type))
			try {
				conn.setDouble(1, test_double);
			}catch (UnsupportedOperationException e) {
				assertTrue(e.getMessage().contains("Trying to set"));
			}
		else if ("smallint".equals(table_type))
			try {
				conn.setDate(1, test_date);
			}catch (UnsupportedOperationException e) {
				assertTrue(e.getMessage().contains("Trying to set"));
			}
		else if ("int".equals(table_type))
			try {
				conn.setDate(1, test_date);
			}catch (UnsupportedOperationException e) {
				assertTrue(e.getMessage().contains("Trying to set"));
			}
		else if ("bigint".equals(table_type))
			try {
				conn.setDate(1, test_date);
			}catch (UnsupportedOperationException e) {
				assertTrue(e.getMessage().contains("Trying to set"));
			}
		else if ("real".equals(table_type))
			try {
				conn.setDate(1, test_date);
			}catch (UnsupportedOperationException e) {
				assertTrue(e.getMessage().contains("Trying to set"));
			}
		else if ("double".equals(table_type))
			try {
			 	conn.setFloat(1, test_real);
			}catch (UnsupportedOperationException e) {
				assertTrue(e.getMessage().contains("Trying to set"));
			}
		else if ("varchar(100)".equals(table_type))
			try {
				conn.setNvarchar(1, test_nvarchar);
			}catch (UnsupportedOperationException e) {
				assertTrue(e.getMessage().contains("Trying to set"));
			}
		else if ("nvarchar(100)".equals(table_type))
			try {
				conn.setVarchar(1, test_varchar);
			}catch (UnsupportedOperationException e) {
				assertTrue(e.getMessage().contains("Trying to set"));
			}
		else if ("date".equals(table_type))
			try {
				conn.setDatetime(1, test_datetime);
			}catch (UnsupportedOperationException e) {
				assertTrue(e.getMessage().contains("Trying to set"));
			}
		else if ("datetime".equals(table_type))
			try {
				conn.setDate(1, test_date);
			}catch (UnsupportedOperationException e) {
				assertTrue(e.getMessage().contains("Trying to set"));
			}
		conn.close();
    }


    public boolean wrong_type_get(String table_type) throws IOException, KeyManagementException, NoSuchAlgorithmException, ScriptException, ConnException{
    	/* Set a column value, and try to get it back using the wrong get command.
    	   See if error message is correct */

    	boolean a_ok = false;
    	String table_name = table_type.contains("varchar(100)") ?  table_type.substring(0,7) : table_type;
		ConnectorImpl conn = new ConnectorImpl(
				ConnectionParams.builder()
						.ipAddress(IP)
						.port(String.valueOf(PORT))
						.cluster(String.valueOf(CLUSTER))
						.useSsl(String.valueOf(SSL))
						.build());
		conn.connect(DATABASE, USER, PASS, SERVICE);


    	// Prepare Table
    	log.info(" - Create Table t_" + table_type);
    	String sql = MessageFormat.format("create or replace table t_{0} (x {1})", table_name, table_type);
		conn.execute(sql);
		conn.close();
		//Random r = new Random();

		// Insert value
		log.info(" - Insert test value " + table_type);
		sql = MessageFormat.format("insert into t_{0} values (?)", table_name);
		conn.execute(sql);
		if (table_type == "bool")
			conn.setBoolean(1, test_bool);
		else if (table_type == "tinyint")
			conn.setUbyte(1, test_ubyte);
		else if (table_type == "smallint")
			conn.setShort(1, test_short);
		else if (table_type == "int")
			conn.setInt(1, test_int);
		else if (table_type == "bigint")
			conn.setLong(1, test_long);
		else if (table_type == "real")
			conn.setFloat(1, test_real);
		else if (table_type == "double")
			conn.setDouble(1, test_double);
		else if (table_type == "varchar(100)")
			conn.setVarchar(1, test_varchar);
		else if (table_type == "nvarchar(100)")
			conn.setNvarchar(1, test_nvarchar);
		else if (table_type == "date")
			conn.setDate(1, test_date);
		else if (table_type == "datetime")
			conn.setDatetime(1, test_datetime);

		conn.next();
		conn.close();

		// Retreive using wrong statement
		log.info(" - Getting " + table_type + " value back");
		sql = MessageFormat.format("select * from t_{0}", table_name);
		conn.execute(sql);
		// int res = conn.get_int(1);
		//*
		while(conn.next())
		{
			if (table_type == "bool") {
				res_ubyte = conn.getUbyte(1);
			}
			else if (table_type == "tinyint") {
				res_double = conn.getDouble(1);
			}
			else if (table_type == "smallint") {
				res_ubyte = conn.getUbyte(1);
			}
			else if (table_type == "int") {
				res_short = conn.getShort(1);
			}
			else if (table_type == "bigint") {
				res_int = conn.getInt(1);
			}
			else if (table_type == "real") {
				res_long = conn.getLong(1);
			}
			else if (table_type == "double") {
				res_real = conn.getFloat(1);
				//res_double = conn.get_double(1);
			}
			else if (table_type == "varchar(100)") {
				res_nvarchar = conn.getNvarchar(1);
			}
			else if (table_type == "nvarchar(100)") {
				res_int = conn.getInt(1);
			}
			else if (table_type == "date") {
				res_datetime = conn.getDatetime(1);
			}
			else if (table_type == "datetime") {
				res_date = conn.getDate(1);
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


    private void bad_value_set(String table_type) throws IOException, KeyManagementException, NoSuchAlgorithmException, ScriptException, ConnException {
    	/* Try to set a varchar/nvarchar of the wrong size. See if error message is correct */

    	String tableName = table_type.contains("varchar(10)") ?  table_type.substring(0,7) : table_type;

		ConnectorImpl conn = new ConnectorImpl(
				ConnectionParams.builder()
						.ipAddress(IP)
						.port(String.valueOf(PORT))
						.cluster(String.valueOf(CLUSTER))
						.useSsl(String.valueOf(SSL))
						.build());
		conn.connect(DATABASE, USER, PASS, SERVICE);

    	// Prepare Table
    	String sql = MessageFormat.format("create or replace table t_{0} (x {1})", tableName, table_type);
		conn.execute(sql);
		conn.close();

		// Insert a String that is too long - attempts kept for future reference
		if ("tinyint".equals(table_type))
			for (byte bad: bad_ubytes) {
				sql = MessageFormat.format("insert into t_{0} values (?)", tableName);
				conn.execute(sql);

				try {
					conn.setUbyte(1, bad);
				}catch (IllegalArgumentException e) {
					assertTrue(e.getMessage().contains("Trying to set"));
				}
				conn.close();
			}

		else if (varchar_type.equals(table_type))
			for (String bad: badVarchars) {
				sql = MessageFormat.format("insert into t_{0} values (?)", tableName);
				conn.execute(sql);

				try {
					conn.setVarchar(1, bad);}
				catch (IllegalArgumentException e) {
					assertTrue(e.getMessage().contains("Trying to set string of size"));
				}
				conn.close();
			}

		else if ("date".equals(table_type))
			for (Date bad: badDates) {
				sql = MessageFormat.format("insert into t_{0} values (?)", tableName);
				conn.execute(sql);

				try {
					conn.setDate(1, bad);}
				finally {
					conn.close();
				}
				conn.next();
				conn.close();
			}

		else if ("datetime".equals(table_type))
			for (Timestamp bad: badDatetimes) {
				try {
					log.info("Attempted bad insert value: " + bad);
					conn.setDatetime(1, bad);}
				finally {
					conn.close();
				}
				conn.next();
				conn.close();
			}
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

    @Test
    public void wrongTypeTest() throws KeyManagementException, ScriptException, NoSuchAlgorithmException, ConnException, IOException {
		String[] typelist = {"bool", "tinyint", "smallint", "int", "bigint", "real", "double", "date", "datetime"};
		for (String col_type : typelist) {
			wrong_type_set(col_type);
		}
	}

	@Test
	public void badTypeTest() throws KeyManagementException, ScriptException, NoSuchAlgorithmException, ConnException, IOException {
		String[] bad_typelist = {"tinyint", varchar_type};
		for (String table_type: bad_typelist) {
			bad_value_set(table_type);
		}
	}
}



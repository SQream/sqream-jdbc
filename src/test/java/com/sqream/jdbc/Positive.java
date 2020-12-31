package com.sqream.jdbc;

import com.sqream.jdbc.connector.ConnectorImpl;
import com.sqream.jdbc.connector.ConnException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import javax.script.ScriptException;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.MessageFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Random;
import java.util.UUID;
import java.util.logging.Logger;

import static com.sqream.jdbc.TestEnvironment.*;
import static com.sqream.jdbc.TestEnvironment.SERVICE;
import static org.junit.Assert.*;

@RunWith(JUnit4.class)
public class Positive {

	private static final Logger log = Logger.getLogger(Positive.class.toString());

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

	static long time() {
		return System.currentTimeMillis();
	}

	/**
	 * Test that get_varchar returns corect results for all types
	 */
	@Test
	public void varcharTest() throws ConnException {
		ConnectorImpl conn = new ConnectorImpl(
				ConnectionParams.builder()
						.ipAddress(IP)
						.port(String.valueOf(PORT))
						.cluster(String.valueOf(CLUSTER))
						.useSsl(String.valueOf(SSL))
						.build());
		conn.connect(DATABASE, USER, PASS, SERVICE);

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
			String res = conn.getNvarchar(11);
			assertFalse(conn.getBoolean(1));
			assertEquals(Byte.valueOf((byte) 14), conn.getUbyte(2));
			assertEquals(Short.valueOf((short) 140), conn.getShort(3));
			assertEquals(Integer.valueOf(1400), conn.getInt(4));
			assertEquals(Long.valueOf(14000000L), conn.getLong(5));
			assertEquals(Float.valueOf(14.1f), conn.getFloat(6));
			assertEquals(Double.valueOf(14.12345), conn.getDouble(7));
			assertEquals("2013-11-23", conn.getDate(8).toString());
			assertEquals("2013-11-23 14:56:47.1", conn.getDatetime(9).toString());
			assertEquals("wuzz", conn.getVarchar(10).trim());
			assertEquals("up", res);
		}
		conn.close();
	}

	private void insert(String table_type) throws IOException, ScriptException, ConnException, NoSuchAlgorithmException, KeyManagementException {

		String table_name = table_type.contains("varchar(") ? table_type.substring(0, 7) : table_type;
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

		// Insert value
		if ("bool".equals(table_type))
			for (boolean test : test_bools) {
				test_bool = test;
				sql = MessageFormat.format("insert into t_{0} values (?)", table_name);
				conn.execute(sql);

				conn.setBoolean(1, test_bool);
				send_and_retreive_result(conn, table_name, table_type);
				is_identical(table_type);
			}
		else if ("tinyint".equals(table_type))
			for (byte test : test_ubytes) {
				test_ubyte = test;
				sql = MessageFormat.format("insert into t_{0} values (?)", table_name);
				conn.execute(sql);

				conn.setUbyte(1, test_ubyte);
				send_and_retreive_result(conn, table_name, table_type);
				is_identical(table_type);
			}
		else if ("smallint".equals(table_type))
			for (short test : test_shorts) {
				test_short = test;
				sql = MessageFormat.format("insert into t_{0} values (?)", table_name);
				conn.execute(sql);

				conn.setShort(1, test_short);
				send_and_retreive_result(conn, table_name, table_type);
				is_identical(table_type);
			}
		else if ("int".equals(table_type))
			for (int test : test_ints) {
				test_int = test;
				sql = MessageFormat.format("insert into t_{0} values (?)", table_name);
				conn.execute(sql);
				conn.setInt(1, test_int);
				send_and_retreive_result(conn, table_name, table_type);
				is_identical(table_type);
			}
		else if ("bigint".equals(table_type))
			for (long test : test_longs) {
				test_long = test;
				sql = MessageFormat.format("insert into t_{0} values (?)", table_name);
				conn.execute(sql);

				conn.setLong(1, test_long);
				send_and_retreive_result(conn, table_name, table_type);
				is_identical(table_type);
			}
		else if ("real".equals(table_type))
			for (float test : test_reals) {
				test_real = test;
				sql = MessageFormat.format("insert into t_{0} values (?)", table_name);
				conn.execute(sql);

				conn.setFloat(1, test_real);
				send_and_retreive_result(conn, table_name, table_type);
				is_identical(table_type);
			}
		else if ("double".equals(table_type))
			for (double test : test_doubles) {
				test_double = test;
				sql = MessageFormat.format("insert into t_{0} values (?)", table_name);
				conn.execute(sql);

				conn.setDouble(1, test_double);
				send_and_retreive_result(conn, table_name, table_type);
				is_identical(table_type);
			}
		else if ("varchar(100)".equals(table_type))
			for (String test : test_varchars) {
				test_varchar = test;
				sql = MessageFormat.format("insert into t_{0} values (?)", table_name);
				conn.execute(sql);

				conn.setVarchar(1, test_varchar);
				send_and_retreive_result(conn, table_name, table_type);
				is_identical(table_type);
			}
		else if ("nvarchar(4)".equals(table_type))
			for (String test : test_varchars) {
				test_varchar = test;
				sql = MessageFormat.format("insert into t_{0} values (?)", table_name);
				conn.execute(sql);

				conn.setNvarchar(1, test_nvarchar);
				send_and_retreive_result(conn, table_name, table_type);
				is_identical(table_type);
			}
		else if ("date".equals(table_type))
			for (Date test : test_dates) {
				test_date = test;
				sql = MessageFormat.format("insert into t_{0} values (?)", table_name);
				conn.execute(sql);

				conn.setDate(1, test_date);
				send_and_retreive_result(conn, table_name, table_type);
				is_identical(table_type);
			}
		else if ("datetime".equals(table_type))
			for (Timestamp test : test_datetimes) {
				test_datetime = test;
				sql = MessageFormat.format("insert into t_{0} values (?)", table_name);
				conn.execute(sql);

				conn.setDatetime(1, test_datetime);
				send_and_retreive_result(conn, table_name, table_type);
				is_identical(table_type);
			}
	}

    public void send_and_retreive_result (ConnectorImpl conn, String table_name, String table_type) throws ConnException, IOException, ScriptException, NoSuchAlgorithmException, KeyManagementException {

    	conn.next();
		conn.close();
		//*
		// Retreive
		// log.info(" - Getting " + table_type + " value back for value");
		String sql = MessageFormat.format("select * from t_{0}", table_name);
		conn.execute(sql);

		//log.info("type_out: " + type_out);

		// int res = conn.get_int(1);
		//*
		while(conn.next())
		{
			if (table_type == "bool")
				res_bool = conn.getBoolean(1);
			else if (table_type == "tinyint")
				res_ubyte = conn.getUbyte(1);
			else if (table_type == "smallint")
				res_short = conn.getShort(1);
			else if (table_type == "int")
				res_int = conn.getInt(1);
			else if (table_type == "bigint")
				res_long = conn.getLong(1);
			else if (table_type == "real")
				res_real = conn.getFloat(1);
			else if (table_type == "double")
				res_double = conn.getDouble(1);
			else if (table_type == "varchar(100)")
				res_varchar = conn.getVarchar(1);
			else if (table_type == "nvarchar(4)")
				res_nvarchar = conn.getNvarchar(1);
			else if (table_type == "date")
				res_date = conn.getDate(1);
			else if (table_type == "datetime")
				res_datetime = conn.getDatetime(1);
		}  //*/
		conn.close();
	}

	public void is_identical(String table_type) {
		if ("bool".equals(table_type))
			assertEquals(msgNotIdentical(table_type, test_bool, res_bool), test_bool, res_bool);
		else if ("tinyint".equals(table_type))
			assertEquals(msgNotIdentical(table_type, test_ubyte, res_ubyte), test_ubyte, res_ubyte);
		else if ("smallint".equals(table_type))
			assertEquals(msgNotIdentical(table_type, test_short, res_short), test_short, res_short);
		else if ("int".equals(table_type))
			assertEquals(msgNotIdentical(table_type, test_int, res_int), test_int, res_int);
		else if ("bigint".equals(table_type))
			assertEquals(msgNotIdentical(table_type, test_long, res_long), test_long, res_long);
		else if ("real".equals(table_type))
			assertEquals(msgNotIdentical(table_type,test_real, res_real) ,test_real, res_real, 0);
		else if ("double".equals(table_type))
			assertEquals(msgNotIdentical(table_type, test_double, res_double), test_double, res_double, 0);
		else if ("varchar(100)".equals(table_type))
			assertEquals(msgNotIdentical(table_type, test_varchar, res_varchar), test_varchar, res_varchar.trim());
		else if ("nvarchar(4)".equals(table_type))
			assertEquals(msgNotIdentical(table_type, test_varchar, res_varchar), test_nvarchar, res_nvarchar.trim());
		else if ("date".equals(table_type))
			assertEquals(msgNotIdentical(table_type, test_date, res_date), 1, Math.abs(test_date.compareTo(res_date)));
		else if ("datetime".equals(table_type))
			assertEquals(msgNotIdentical(table_type, test_datetime, res_datetime), test_datetime, res_datetime);
	}

	private String msgNotIdentical(String tableType, Object expected, Object actual) {
		return MessageFormat.format("Results not identical. Table type [{0}], expected [{1}], actual [{2}]",
				tableType, expected, actual);
	}


    private boolean autoflush(int total_inserts, int insert_every) throws IOException, ScriptException, ConnException, NoSuchAlgorithmException, KeyManagementException   {

		ConnectorImpl conn = new ConnectorImpl(
				ConnectionParams.builder()
						.ipAddress(IP)
						.port(String.valueOf(PORT))
						.cluster(String.valueOf(CLUSTER))
						.useSsl(String.valueOf(SSL))
						.build());
		conn.connect(DATABASE, USER, PASS, SERVICE);

    	// Prepare Table
    	String table_type = "int";

    	//int row_num = 100000000;
    	String sql = MessageFormat.format("create or replace table t_{0} (x {0})", table_type);
    	sql = "create or replace table test (x int, y nvarchar(50))";
		conn.execute(sql);
		conn.close();

		sql = MessageFormat.format("insert into test values (?, ?)", table_type);
		conn.execute(sql);

		int multi_row_value = 8;
		String multi_row_string = "koko";
    	//* Insert a bunch of rows
		for(int i =0 ; i< total_inserts; i++) {
          conn.setInt(1,  multi_row_value);
          conn.setNvarchar(2,  multi_row_string);
          conn.next();  //  if ((i)%commitEvery==0)

		}
		conn.close();
    	return true;
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

	 @Test
	 public void someTest() throws KeyManagementException, ScriptException, NoSuchAlgorithmException, ConnException, IOException {
		 String[] typelist = {"bool", "tinyint", "smallint", "int", "bigint", "real", "double", "varchar(100)", "nvarchar(4)", "date", "datetime"};

		 for (String col_type : typelist) {
			 insert(col_type);
		 }
	 }

	 @Test
	 public void autoFlushTest() throws KeyManagementException, ScriptException, NoSuchAlgorithmException, ConnException, IOException {
		 assertTrue(autoflush(1000000, 50000));
	 }
}



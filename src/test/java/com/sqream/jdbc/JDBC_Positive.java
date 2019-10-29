package com.sqream.jdbc;

import java.text.MessageFormat;
import java.time.LocalTime;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
//import org.junit.Assert;
//import org.junit.Test;
import java.util.stream.IntStream;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ParameterMetaData;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.PreparedStatement;
import java.sql.DatabaseMetaData;    // for getTables test
import java.sql.ResultSetMetaData;
import java.sql.Types;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;

//Datetime related
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.TimeZone;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;

import com.sqream.jdbc.Connector;


public class JDBC_Positive {
    
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
    Timestamp[] test_datetimes = {new Timestamp(315711884629l)};    

    boolean   res_bool     = true;
    byte      res_ubyte    = 0;
    short     res_short    = 0;
    int       res_int      = 0;
    long      res_long     = 0l;
    float     res_real     = r.nextFloat();
    double    res_double   = 0.0;
    String    res_varchar  = "";
    //String test_varchar = "koko"; 
    Date      res_date     = date_from_tuple(2012, 9, 13);
    Timestamp res_datetime = datetime_from_tuple(2002, 9, 13, 14, 56, 34, 567);
    
    ///*
    boolean test_bool = false;
    byte test_ubyte = 15;
    short test_short = 500;
    int test_int = r.nextInt();
    long test_long = r.nextLong();
    float test_real = r.nextFloat();
    double test_double = r.nextDouble();
    String test_varchar = UUID.randomUUID().toString();
    //String test_varchar = "koko"; 
    Date test_date = date_from_tuple(2002, 9, 13);
    Timestamp test_datetime = datetime_from_tuple(2002, 9, 13, 14, 56, 34, 567); //*/
    
    // JDBC Data
    static final String url = "jdbc:Sqream://127.0.0.1:5000/master;user=sqream;password=sqream;cluster=false;ssl=false;service=sqream";
    
    Statement stmt = null;
    ResultSet rs = null;
    Connection conn  = null;
    PreparedStatement ps = null;
    
    // For testTables() test
    DatabaseMetaData dbmeta = null;
    ResultSetMetaData rsmeta = null;
    // Load JDBC driver
    
    static void print(Object printable) {
		System.out.println(printable);
	}
	
	static void printbuf(ByteBuffer to_print, String description) {
		print(description + " : " + to_print);
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
	
	
	public boolean limited_fetch() throws SQLException {
		boolean a_ok = false;  // The test is visual, pass if ends
		int count = 0;
		
		conn = DriverManager.getConnection(url,"sqream","sqream");
		
		String sql = "create or replace table test_fetch (ints int)";
	    stmt = conn.createStatement();
	    stmt.execute(sql);
	    stmt.close();
	    
	    sql = "insert into test_fetch values (1), (2), (3), (4), (5)";
	    stmt = conn.createStatement();
	    stmt.execute(sql);
	    stmt.close();
	    
	    sql = "select * from test_fetch";
	    //stmt = conn.prepareStatement(sql);
	    stmt = conn.createStatement();
	    stmt.setMaxRows(3);
	    rs = stmt.executeQuery(sql);
	    while(rs.next()) { 
	        rs.getInt(1);
	        count++;
	    }
	    
	    if (count == 3)
            a_ok = true;    
        else
        	print("limited fetch of 3 items, amount returned: " + count);
        
	    
	    return a_ok;
	}
	
	
	
	public boolean pre_fetch() throws SQLException {
		boolean a_ok = true;  // The test is visual, pass if ends
		
		conn = DriverManager.getConnection(url,"sqream","sqream");
		
		String sql = "create or replace table test_autoclose (ints int)";
	    stmt = conn.createStatement();
	    stmt.execute(sql);
	    stmt.close();
	    
	    sql = "insert into test_autoclose values (1), (2), (3), (4), (5)";
	    stmt = conn.createStatement();
	    stmt.execute(sql);
	    stmt.close();
	    
	    sql = "select * from test_autoclose where 1 = 0";
	    //stmt = conn.prepareStatement(sql);
	    stmt = conn.createStatement();
	    rs = stmt.executeQuery(sql);
	    
	    sql = "select * from test_autoclose";
	    stmt = conn.createStatement();
	    rs = stmt.executeQuery(sql);
	    while(rs.next()) 
	        print ("get result :" + rs.getInt(1));
	    // rs.close();
	    // stmt.close();
	    
	    //TimeUnit.SECONDS.sleep(1);
	    
	    return a_ok;
	}
	    
	
	public boolean display_size() throws SQLException {
        boolean a_ok = false;
        int[] res;
        
        // Create table for test
        conn = DriverManager.getConnection(url,"sqream","sqream");
        String sql = "create or replace table test_display (x nvarchar(11))";
        stmt = conn.createStatement();
        stmt.execute(sql);
        stmt.close();
        
        conn = DriverManager.getConnection(url,"sqream","sqream");
        sql = "select * from test_display";
        stmt = conn.createStatement();
        rs = stmt.executeQuery(sql);
        rsmeta = rs.getMetaData();
        rs.close();
        stmt.close();
        
        // Check result
        if (rsmeta.getColumnDisplaySize(1) == 11)
            a_ok = true;    
        else
        	print("nvarchar(11) display size should be 11 but got " +  rsmeta.getColumnDisplaySize(1));
        
        return a_ok;
    }
	
	
    public boolean execBatchRes() throws SQLException {
        boolean a_ok = false;
        int[] res;
        
        // Create table for test
        conn = DriverManager.getConnection(url,"sqream","sqream");
        String sql = "create or replace table test_exec (x int)";
        stmt = conn.createStatement();
        stmt.execute(sql);
        stmt.close();
        
        // Insert an int via network insert
        sql = "insert into test_exec values (?)";
        ps = conn.prepareStatement(sql);
        int random_int = 8;
        int times = 10;
        for (int i = 0; i < times; i++) {
            ps.setInt(1, random_int);
            ps.addBatch();
        }
        res = ps.executeBatch();
        ps.close();
        
        // Check result
        if (res.length == 10 && IntStream.of(res).sum() == 10)
            a_ok = true;    
        
        return a_ok;
    }
    
    public boolean is_logging_off () {
    	return !Connector.is_logging();
    }
    
    public void get_connection () throws SQLException {
    	conn = DriverManager.getConnection(url,"sqream","sqream");
    	
    }
     
    public boolean parameter_metadata() throws SQLException {
        /*  Check if charitable behavior works - not closing statement before starting the next one   */
        
   	 boolean a_ok = true;
        
        // Count test - DML
        conn = DriverManager.getConnection(url,"sqream","sqream");
        String sql = "create or replace table test_parameter(bools bool not null, tinies tinyint, smalls smallint, ints int, bigs bigint, floats real, doubles double, dates date, dts datetime, varcs varchar (10), nvarcs nvarchar (10))";
        ps = conn.prepareStatement(sql);
        ParameterMetaData params = ps.getParameterMetaData();
        int count = params.getParameterCount() ;
        if (count != 0) {
        	print ("Should have 0 parameter count on a DML query, but got: " + count);
        	a_ok = false;
        }
    	ps.close();
       
    	// Count test - regular insert
        sql = "insert into test_parameter values (true, 1, 11, 111, 1111, 1.1, 1.11, '2016-11-03', '2016-11-03 16:56:45.000', 'bla', 'nbla')";
        ps = conn.prepareStatement(sql);
        params = ps.getParameterMetaData();
        count = params.getParameterCount() ;
        if (count != 0) {
        	print ("Should have 0 parameter count on a regular insert query, but got: " + count);
	        a_ok = false;
	    }
        ps.close();
        
        // Network insert - an actual paramtered query
        sql = "insert into test_parameter values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        ps = conn.prepareStatement(sql);
        params = ps.getParameterMetaData();
        
        count = params.getParameterCount() ;
        if (count != 11) {
        	print ("Should have 3 parameter count on a network insert, but got: " + count);
        	a_ok = false;
        }
        
    	if (!params.getParameterClassName(1).equals("denied") || !params.getParameterClassName(2).equals("denied") || 
			!params.getParameterClassName(3).equals("denied") || !params.getParameterClassName(4).equals("denied") || 
			!params.getParameterClassName(5).equals("denied") || !params.getParameterClassName(6).equals("denied") || 
			!params.getParameterClassName(7).equals("denied") || !params.getParameterClassName(8).equals("denied") || 
			!params.getParameterClassName(9).equals("denied") || !params.getParameterClassName(10).equals("denied") || 
			!params.getParameterClassName(11).equals("denied"))
    	{
    		print ("Bad column names returned:\n" + params.getParameterClassName(1) + '\n' + params.getParameterClassName(2) + '\n' + params.getParameterClassName(3) + '\n' + params.getParameterClassName(4) + '\n' + params.getParameterClassName(5) + '\n' + params.getParameterClassName(6) + '\n' + params.getParameterClassName(7) + '\n' + params.getParameterClassName(8) + '\n' + params.getParameterClassName(9) + '\n' + params.getParameterClassName(10) + '\n' + params.getParameterClassName(11)  );
        	a_ok = false;
    	}
    	
    	if (params.getParameterMode(1) != ParameterMetaData.parameterModeIn || params.getParameterMode(2) != ParameterMetaData.parameterModeIn  || 
			params.getParameterMode(3) != ParameterMetaData.parameterModeIn || params.getParameterMode(4) != ParameterMetaData.parameterModeIn  || 
			params.getParameterMode(5) != ParameterMetaData.parameterModeIn || params.getParameterMode(6) != ParameterMetaData.parameterModeIn  || 
			params.getParameterMode(7) != ParameterMetaData.parameterModeIn || params.getParameterMode(8) != ParameterMetaData.parameterModeIn  || 
			params.getParameterMode(9) != ParameterMetaData.parameterModeIn || params.getParameterMode(10) != ParameterMetaData.parameterModeIn  || 
			params.getParameterMode(11) != ParameterMetaData.parameterModeIn) 
    	{
    		print ("Bad parameter mode returned: " + params.getParameterMode(1));
        	a_ok = false;
    	}
    	
    	if (params.getScale(1) != 0 || params.getScale(2) != 0 || params.getScale(3) != 0 || 
			params.getScale(4) != 0 || params.getScale(5) != 0 || params.getScale(6) != 0 || 
			params.getScale(7) != 0 || params.getScale(8) != 0 || params.getScale(9) != 0 || 
			params.getScale(10) != 0 || params.getScale(11) != 0)
		{
    		// 4 on float, 8 on double, 0 elsewhere
    		print ("Bad scale returned: " + params.getScale(2));
        	a_ok = false;
    	}
    	
    	if (params.isNullable(1) != ParameterMetaData.parameterNullableUnknown || params.isNullable(2) != ParameterMetaData.parameterNullable ||
			params.isNullable(3) != ParameterMetaData.parameterNullable || params.isNullable(4) != ParameterMetaData.parameterNullable ||
			params.isNullable(5) != ParameterMetaData.parameterNullable || params.isNullable(6) != ParameterMetaData.parameterNullable ||
			params.isNullable(7) != ParameterMetaData.parameterNullable || params.isNullable(8) != ParameterMetaData.parameterNullable ||
			params.isNullable(9) != ParameterMetaData.parameterNullable || params.isNullable(10) != ParameterMetaData.parameterNullable ||
			params.isNullable(11) != ParameterMetaData.parameterNullable)
		{
    		// int column is not nullable
    		print ("Bad isNullable returned: " + params.isNullable(1));
        	a_ok = false;
    	}
    	
    	if (params.getParameterType(1) != Types.BOOLEAN || params.getParameterType(2) != Types.TINYINT || 
			params.getParameterType(3) != Types.SMALLINT || params.getParameterType(4) != Types.INTEGER || 
			params.getParameterType(5) != Types.BIGINT || params.getParameterType(6) != Types.REAL || 
			params.getParameterType(7) != Types.DOUBLE || params.getParameterType(8) != Types.DATE || 
			params.getParameterType(9) != Types.TIMESTAMP || params.getParameterType(10) != Types.VARCHAR || 
			params.getParameterType(11) != Types.NVARCHAR)
		{
    		print ("Bad parameter type returned: " + params.isNullable(1));
        	a_ok = false;
    	}
    	
    	//params.getParameterType(1)
    	if (params.getPrecision(1) != 1 || params.getPrecision(2) != 1 || params.getPrecision(3) != 2 || 
			params.getPrecision(4) != 4 || params.getPrecision(5) != 8 || params.getPrecision(6) != 4 || 
			params.getPrecision(7) != 8 || params.getPrecision(8) != 4 || params.getPrecision(9) != 8 || 
			params.getPrecision(10) != 10 || params.getPrecision(11) != 40)
		{
    		print ("Bad precision returned from parameter test:\n" + params.getPrecision(1) + '\n' + params.getPrecision(2) + '\n' + params.getPrecision(3) + '\n' + params.getPrecision(4) + '\n' + params.getPrecision(5) + '\n' + params.getPrecision(6) + '\n' + params.getPrecision(7) + '\n' + params.getPrecision(8) + '\n' + params.getPrecision(9) + '\n' + params.getPrecision(10) + '\n' + params.getPrecision(11)  );
        	a_ok = false;
    	}
    	
    	if (!params.getParameterTypeName(1).equals("ftBool") || !params.getParameterTypeName(2).equals("ftUByte") || 
			!params.getParameterTypeName(3).equals("ftShort") || !params.getParameterTypeName(4).equals("ftInt") || 
			!params.getParameterTypeName(5).equals("ftLong") || !params.getParameterTypeName(6).equals("ftFloat") || 
			!params.getParameterTypeName(7).equals("ftDouble") || !params.getParameterTypeName(8).equals("ftDate") || 
			!params.getParameterTypeName(9).equals("ftDateTime") || !params.getParameterTypeName(10).equals("ftVarchar") || 
			!params.getParameterTypeName(11).equals("ftBlob"))
    	{
    		print ("Bad taypenames returned:\n" + params.getParameterTypeName(1) + '\n' + params.getParameterTypeName(2) + '\n' + params.getParameterTypeName(3) + '\n' + params.getParameterTypeName(4) + '\n' + params.getParameterTypeName(5) + '\n' + params.getParameterTypeName(6) + '\n' + params.getParameterTypeName(7) + '\n' + params.getParameterTypeName(8) + '\n' + params.getParameterTypeName(9) + '\n' + params.getParameterTypeName(10) + '\n' + params.getParameterTypeName(11)  );
        	a_ok = false;
    	}
    	
    	//params.getParameterType(1)
    	if (params.isSigned(1) != false || params.isSigned(2) != false || params.isSigned(3) != true ||
			params.isSigned(4) != true || params.isSigned(5) != true || params.isSigned(6) != true ||		
			params.isSigned(7) != true || params.isSigned(8) != false || params.isSigned(9) != false ||
			params.isSigned(10) != false || params.isSigned(11) != false)
    	{
    		print ("Bad values returned on isSigned():" + params.isSigned(1));
        	a_ok = false;
    	}
    	
    	ps.close();
        
        
        return a_ok;
    }
    
    
    public boolean not_closing() throws SQLException {
        /*  Check if charitable behavior works - not closing statement before starting the next one   */
        
   	    boolean a_ok = false;
        
        // Create some user defined functions
        conn = DriverManager.getConnection(url,"sqream","sqream");
        String sql = "select 1";
        stmt = conn.createStatement();
        stmt.execute(sql);
        // stmt.close();
               
        sql = "select 2";
        stmt = conn.createStatement();
        rs = stmt.executeQuery(sql);

        rs.next();

        if (rs.getInt(1) == 2)
        	a_ok = true;
        rs.close();
        stmt.close();
        
        
        return a_ok;
    }
    
    
    public boolean bool_as_string() throws SQLException {
    	
    	boolean a_ok = false;
    	
        conn = DriverManager.getConnection(url,"sqream","sqream");
        String sql = "create or replace table bool_string (x bool, y bool)";
        stmt = conn.createStatement();
        stmt.execute(sql);
        stmt.close();
        
        sql = "insert into bool_string values (true, false)";
        stmt = conn.createStatement();
        stmt.execute(sql);
        stmt.close();
    	
        sql = "select * from bool_string";
        stmt = conn.createStatement();
        rs = stmt.executeQuery(sql);
        rs.next();
        if (rs.getString(1).equals("true") && rs.getString(2).equals("false"))
        	a_ok = true;
        
        rs.close();
        stmt.close();
        
        
    	return a_ok;
    }
    
    
    
     public boolean getUDF() throws SQLException {
        /*  Check isSigned command()   */
        boolean a_ok = false;
        
        // Create some user defined functions
        conn = DriverManager.getConnection(url,"sqream","sqream");
        String sql = "CREATE OR REPLACE FUNCTION fud () RETURNS int as $$ return 1 $$ LANGUAGE PYTHON";
        stmt = conn.createStatement();
        stmt.execute(sql);
        stmt.close();
               
        // Run getProcedures
        dbmeta = conn.getMetaData();
        rs = dbmeta.getProcedures(null, null, null);
        String udfName = "";
        while(rs.next()) 
            udfName = rs.getString("procedure_name"); 
        // Check functionality
        if (udfName.equals("fud"))
            a_ok = true;    
       
        rs.close();
        
        return a_ok;
    }

    public boolean isSigned() throws SQLException {
        /*  Check isSigned command()   */
        boolean a_ok = false;
        
        // Create table for test
        conn = DriverManager.getConnection(url,"sqream","sqream");
        String sql = "create or replace table test_signed (x int, y varchar(10))";
        stmt = conn.createStatement();
        stmt.execute(sql);
        stmt.close();
        
        // Run select statement and check metadata
        stmt = conn.createStatement();
        rs = stmt.executeQuery("select * from test_signed");
        rsmeta = rs.getMetaData();
        
        // Check functionality
        if (rsmeta.isSigned(1) && !rsmeta.isSigned(2))
            a_ok = true;    
        rs.close();
        stmt.close();
        
        
        return a_ok;
    }
    
    
    public boolean timeZones() throws SQLException {
        /*  Check isSigned command()   */
        boolean a_ok = false;
        
        // Create table for test
        conn = DriverManager.getConnection(url,"sqream","sqream");
        String sql = "create or replace table test_zones (x date, y datetime)";
        stmt = conn.createStatement();
        stmt.execute(sql);
        stmt.close();
        
        // Set using calendar
        sql = "insert into test_zones values (?, ?)";
        ps = conn.prepareStatement(sql);
        Date date = Date.valueOf(LocalDate.of(2002, 9, 15));
        Timestamp datetime = Timestamp.valueOf(LocalDateTime.of(2002, 9, 23, 13, 40, 34)); 
        String zone = "Europe/Oslo"; //"Asia/Yekaterinburg"; //  "Pacific/Fiji";
        Calendar cal = Calendar.getInstance(TimeZone.getTimeZone(zone)); // +2 in summer, +1 in winter
        
        ps.setDate(1, date, cal);
        ps.setTimestamp(2, datetime, cal);
        ps.addBatch();
        ps.executeBatch();
        ps.close();

        // Get back without calendar
        sql = "select * from test_zones";
        stmt = conn.createStatement();
        rs = stmt.executeQuery(sql);
        rs.next();
        Date resDate = rs.getDate(1);
        Timestamp resDateTime = rs.getTimestamp(2);
        rs.close();
        stmt.close();
        
        // Get back with calendar
        sql = "select * from test_zones";
        stmt = conn.createStatement();
        rs = stmt.executeQuery(sql);
        rs.next();
        Date resDateZoned = rs.getDate(1, cal);
        Timestamp resDateTimeZoned = rs.getTimestamp(2, cal);
        rs.close();
        stmt.close();

        // Compare results - datetime > resDateTime as Oslo > UTC (UTC + 2)
        if (datetime.compareTo(resDateTime) > 0 && datetime.compareTo(resDateTimeZoned) == 0)
            a_ok = true;
        /*
        print("Originals - date: " +  date + " datetime: " + datetime);
        print("Retrieved wo calendar - date: " + resDate + " datetime: " + resDateTime );
        print("Retrieved with calendar- date: " + resDateZoned + " datetime: " + resDateTimeZoned );
        print("Does equal date: " + (date.compareTo(resDateZoned)));
        print("Does equal datetime: " + (datetime.compareTo(resDateTimeZoned)));
        print("Does equal datetime when retreived with no cal: " + (datetime.compareTo(resDateTime)));
        //*/
        // print("a_ok: " + a_ok);
        
        return a_ok;
    }
    
    
    public boolean casted_gets() throws SQLException {
        /*  Check isSigned command()   */
        boolean a_ok = false;
        
        // Create table for test
        conn = DriverManager.getConnection(url,"sqream","sqream");
        String sql = "create or replace table test_casts (x tinyint, y smallint, z real)";
        stmt = conn.createStatement();
        stmt.execute(sql);
        stmt.close();
        
        // Set using calendar
        byte test_byte = 5;
        short test_short = 55;
        float test_float = (float)5.5;
        sql = "insert into test_casts values (?, ?, ?)";
        ps = conn.prepareStatement(sql);
        ps.setByte(1, test_byte);
        ps.setShort(2, test_short);
        ps.setFloat(3, test_float);
        ps.addBatch();
        ps.executeBatch();
        ps.close();

        // Get back without calendar
        sql = "select * from test_casts";
        stmt = conn.createStatement();
        rs = stmt.executeQuery(sql);
        rs.next();
        if (test_byte != rs.getInt(1))
        	print ("bad casted getInt on byte column");
        else if (test_byte != rs.getShort(1))
        	print ("bad casted getShort on byte column");
        else if (test_short != rs.getInt(2))
			print ("bad casted getInt on short column");
        else if (test_float != rs.getDouble(3))
			print ("bad casted getDouble on float column");
        else
        	a_ok = true;
        
        rs.close();
        stmt.close();
       
        
        return a_ok;
    }
    
    
    public boolean insert(String table_type) throws IOException, SQLException, KeyManagementException, NoSuchAlgorithmException{
        
        boolean a_ok = false;
        String table_name = table_type.contains("varchar(100)") ?  table_type.substring(0,7) : table_type;
        table_name = table_name.toUpperCase();
        String sql;

        conn = DriverManager.getConnection(url,"sqream","sqream");
        
        // Prepare Table
//      print(" - Create Table t_" + table_type);
        stmt = conn.createStatement();
        sql = MessageFormat.format("create or replace table t_{0} (Xx {1})", table_name, table_type);
        stmt.execute(sql);
        if (stmt != null){
            stmt.close();
        }
        
        // Insert value
//      print(" - Insert test value " + table_type);
        if (table_type == "bool") 
            for (boolean test : test_bools) {
                test_bool = test;
                sql = MessageFormat.format("insert into t_{0} values (?)", table_name);
                ps = conn.prepareStatement(sql);
                
                ps.setBoolean(1, test_bool); 
                send_and_retreive_result (ps, table_name, table_type);
                a_ok = is_identical(table_type);}
        else if (table_type == "tinyint") 
            for (byte test : test_ubytes) {
                test_ubyte = test;
                sql = MessageFormat.format("insert into t_{0} values (?)", table_name);
                ps = conn.prepareStatement(sql);
                
                ps.setByte(1, test_ubyte);
                send_and_retreive_result (ps, table_name, table_type);
                a_ok = is_identical(table_type);}
        else if (table_type == "smallint") 
            for (short test : test_shorts) {
                test_short = test;
                sql = MessageFormat.format("insert into t_{0} values (?)", table_name);
                ps = conn.prepareStatement(sql);
                
                ps.setShort(1, test_short);
                send_and_retreive_result (ps, table_name, table_type);
                a_ok = is_identical(table_type);}
        else if (table_type == "int") 
            for (int test : test_ints) {
                test_int = test;
                sql = MessageFormat.format("insert into t_{0} values (?)", table_name);
                ps = conn.prepareStatement(sql);
                
                ps.setInt(1, test_int);
                send_and_retreive_result (ps, table_name, table_type);
                a_ok = is_identical(table_type);}
        else if (table_type == "bigint")
            for (long test : test_longs) {
                test_long = test;
                sql = MessageFormat.format("insert into t_{0} values (?)", table_name);
                ps = conn.prepareStatement(sql);
                
                ps.setLong(1, test_long);
                send_and_retreive_result (ps, table_name, table_type);
                a_ok = is_identical(table_type);}
        else if (table_type == "real")
            for (float test : test_reals) {
                test_real = test;
                sql = MessageFormat.format("insert into t_{0} values (?)", table_name);
                ps = conn.prepareStatement(sql);
                
                ps.setFloat(1, test_real);
                send_and_retreive_result (ps, table_name, table_type);
                a_ok = is_identical(table_type);}
        else if (table_type == "double")
            for (double test : test_doubles) {
                test_double = test;
                sql = MessageFormat.format("insert into t_{0} values (?)", table_name);
                ps = conn.prepareStatement(sql);
                
                ps.setDouble(1, test_double);
                send_and_retreive_result (ps, table_name, table_type); 
                a_ok = is_identical(table_type);}
        else if (table_type == "varchar(100)")
            for (String test : test_varchars) {
                test_varchar = test;
                sql = MessageFormat.format("insert into t_{0} values (?)", table_name);
                ps = conn.prepareStatement(sql);
                
                ps.setString(1, test_varchar);
                send_and_retreive_result (ps, table_name, table_type);
                a_ok = is_identical(table_type);}
        else if (table_type == "nvarchar(100)")
            for (String test : test_varchars) {
                test_varchar = test;
                sql = MessageFormat.format("insert into t_{0} values (?)", table_name);
                ps = conn.prepareStatement(sql);
                
                ps.setString(1, test_varchar);
                send_and_retreive_result (ps, table_name, table_type); 
                a_ok = is_identical(table_type);}
        else if (table_type == "date")
            for (Date test : test_dates) {
                test_date = test;
                sql = MessageFormat.format("insert into t_{0} values (?)", table_name);
                ps = conn.prepareStatement(sql);
                
                ps.setDate(1, test_date);
                send_and_retreive_result (ps, table_name, table_type);
                a_ok = is_identical(table_type);}
        else if (table_type == "datetime")
            for (Timestamp test : test_datetimes) {
                test_datetime = test;
                //print("datetime: " + test_datetime);
                sql = MessageFormat.format("insert into t_{0} values (?)", table_name);
                ps = conn.prepareStatement(sql);
                
                ps.setTimestamp(1, test_datetime);
                send_and_retreive_result (ps, table_name, table_type); 
                a_ok = is_identical(table_type);}
    
        return a_ok;
    }
    
    public void send_and_retreive_result (PreparedStatement ps, String table_name, String table_type) throws IOException, SQLException {
        
        ps.addBatch();
        ps.executeBatch();
        
        ps.close();
        
        //*
        // Retreive
//      print(" - Getting " + table_type + " value back for value");
        String sql = MessageFormat.format("select * from t_{0}", table_name);
        stmt = conn.createStatement();
        rs = stmt.executeQuery(sql);
        while(rs.next())
        {
            if (table_type == "bool") {
            	if (rs.getBoolean(1) != rs.getBoolean("Xx")) 
            		print ("Different results on getBoolean on index vs column name");
            	if (!rs.getString("Xx").equals(String.valueOf(rs.getBoolean("Xx"))))
            		print ("Different results on stringified getBoolean vs getString");
            	res_bool = rs.getBoolean(1);
            }else if (table_type == "tinyint") {
            	if (rs.getByte(1) != rs.getByte("Xx")) 
            		print ("Different results on getByte on index vs column name");
            	if (!rs.getString("Xx").equals(String.valueOf(rs.getByte("Xx")))) 
            		print ("Different results on stringified getByte vs getString");
            	if ((short) rs.getByte(1) != rs.getShort(1)) 
            		print ("Different results on getByte vs getShort");
            	if ((int) rs.getByte(1) != rs.getInt(1)) 
            		print ("Different results on getByte vs getInt");
            	if ((long) rs.getByte(1) != rs.getLong(1)) 
            		print ("Different results on getByte vs getLong");
            	if ((float) rs.getByte(1) != rs.getFloat(1)) 
            		print ("Different results on getByte vs getFloat");
            	if ((double) rs.getByte(1) != rs.getDouble(1)) 
            		print ("Different results on getByte vs getDouble");
                res_ubyte = rs.getByte(1);
            }else if (table_type == "smallint") {
            	if (rs.getShort(1) != rs.getShort("Xx")) 
            		print ("Different results on getShort on index vs column name");
            	if (!rs.getString("Xx").equals(String.valueOf(rs.getShort("Xx")))) 
            		print ("Different results on stringified getShort vs getString");
            	if ((int) rs.getShort(1) != rs.getInt(1)) 
            		print ("Different results on getShort vs getInt");
            	if ((long) rs.getShort(1) != rs.getLong(1)) 
            		print ("Different results on getShort vs getLong");
            	if ((float) rs.getShort(1) != rs.getFloat(1)) 
            		print ("Different results on getShort vs getFloat");
            	if ((double) rs.getShort(1) != rs.getDouble(1)) 
            		print ("Different results on getShort vs getDouble");
            	res_short = rs.getShort(1);
            }else if (table_type == "int") {
            	if (rs.getInt(1) != rs.getInt("Xx")) 
            		print ("Different results on getInt on index vs column name");
            	if (!rs.getString("Xx").equals(String.valueOf(rs.getInt("Xx")))) 
            		print ("Different results on stringified getInt vs getString");
            	if ((long) rs.getInt(1) != rs.getLong(1)) 
            		print ("Different results on getInt vs getLong");
            	if ((float) rs.getInt(1) != rs.getFloat(1)) 
            		print ("Different results on getInt vs getFloat");
            	if ((double) rs.getInt(1) != rs.getDouble(1)) 
            		print ("Different results on getInt vs getDouble");
            	res_int = rs.getInt(1);
            }else if (table_type == "bigint") {
            	if (rs.getLong(1) != rs.getLong("Xx")) 
            		print ("Different results on getLong on index vs column name");
            	if (!rs.getString("Xx").equals(String.valueOf(rs.getLong("Xx")))) 
            		print ("Different results on stringified getLong vs getString");
            	if ((double) rs.getLong(1) != rs.getDouble(1)) 
            		print ("Different results on getLong vs getDouble");
            	res_long = rs.getLong(1);
            }else if (table_type == "real") {
            	if (rs.getFloat(1) != rs.getFloat("Xx")) 
            		print ("Different results on getFloat on index vs column name");
            	if (!rs.getString("Xx").equals(String.valueOf(rs.getFloat("Xx")))) 
            		print ("Different results on stringified getFloat vs getString");
            	if ((double) rs.getFloat(1) != rs.getDouble(1)) 
            		print ("Different results on getFloat vs getDouble");
            	res_real = rs.getFloat(1);
            }else if (table_type == "double") {
            	if (rs.getDouble(1) != rs.getDouble("Xx"))
            		print ("Different results on getDouble on index vs column name");
            	if (!rs.getString("Xx").equals(String.valueOf(rs.getDouble("Xx")))) 
            		print ("Different results on stringified getDouble vs getString");
                res_double = rs.getDouble(1);
            }else if (table_type == "varchar(100)") {
            	if (!rs.getString(1).equals(rs.getString("Xx")))
            		print ("Different results on getString on index vs column name");
                res_varchar = rs.getString(1);
            }else if (table_type == "nvarchar(100)") {
            	if (!rs.getString(1).equals(rs.getString("Xx")))
            		print ("Different results on getString on index vs column name");
                res_varchar = rs.getString(1);
            }else if (table_type == "date") {
            	if (Math.abs(rs.getDate(1).compareTo(rs.getDate("Xx"))) > 1) 
            		print ("Different results on getDate on index vs column name");
            	if (!rs.getString("Xx").equals(String.valueOf(rs.getDate("Xx")))) 
            		print ("Different results on stringified getDate vs getString");
                res_date = rs.getDate(1);
            }else if (table_type == "datetime") {
            	if (Math.abs(rs.getTimestamp(1).compareTo(rs.getTimestamp("Xx"))) > 1) 
            		print ("Different results on getTimestamp on index vs column name");
            	if (!rs.getString("Xx").equals(String.valueOf(rs.getTimestamp("Xx")))) 
            		print ("Different results on stringified getTimestamp vs getString");
                res_datetime = rs.getTimestamp(1); 
            }
        }  //*/ 
        rs.close();
        stmt.close();
        
        sql = MessageFormat.format("truncate table t_{0}", table_name);
        stmt = conn.createStatement();
        stmt.execute(sql);
        stmt.close();
    }
    
    public boolean is_identical(String table_type) {
        
        boolean use_junit = false;

        boolean res = false;
        //assertEquals(test_int, res_int);
        /*
        if (use_junit) {
            if (table_type == "bool") {
                Assert.assertEquals(test_bool, res_bool);
    //          print("Results not identical on table type " + table_type + " " + test_bool + " " + res_bool);
            }else if (table_type == "tinyint") {
                Assert.assertEquals(test_ubyte, res_ubyte);
    //          print("Results not identical on table type " + table_type + " " + test_ubyte + " " + res_ubyte);       
            }else if (table_type == "smallint") {
                Assert.assertEquals(test_short, res_short) ;
    //          print("Results not identical on table type " + table_type + " " + test_short + " " + res_short);       
            }else if (table_type == "int") {
                Assert.assertEquals(test_int, res_int); 
    //          print("Results not identical on table type " + table_type + " " + test_int + " " + res_int);       
            }else if (table_type == "bigint") {
                Assert.assertEquals(test_long, res_long) ;
    //          print("Results not identical on table type " + table_type + " " + test_long + " " + res_long);     
            }else if (table_type == "real") {
                Assert.assertEquals(test_real, res_real, 0.0) ;
    //          print("Results not identical on table type " + table_type + " " + test_real + " " + res_real);     
            }else if (table_type == "double") {
                Assert.assertEquals(test_double, res_double, 0.0) ;
    //          print("Results not identical on table type " + table_type + " " + test_double + " " + res_double);     
            }else if (table_type == "varchar(100)") {
                Assert.assertTrue(test_varchar.equals(res_varchar.trim())) ;
    //                  {
    //          print("Results not identical on table type " + table_type + " " + test_varchar + " " + res_varchar);       
    //          print(test_varchar.compareTo(res_varchar) + "a"+ test_varchar.length() + "b" + res_varchar.length());}
            }else if (table_type == "date") {
                    Assert.assertEquals(1, Math.abs(test_date.compareTo(res_date))) ;
    //      {
    //      //else if (table_type == "date" && !test_date.equals(res_date))  {  
    //          print("Results not identical on table type " + table_type + " " + test_date + " " + test_date.getTime() + " " + res_date + " " + res_date.getTime());      
    //          print(test_date.compareTo(res_date));}
            }else if (table_type == "datetime") {
                Assert.assertTrue(test_datetime.equals(res_datetime));
            //else if (table_type == "datetime" && Math.abs(test_datetime.compareTo(res_datetime)) > 1) 
    
    //          print("Results not identical on table type " + table_type + " " + test_datetime + " " + test_datetime.getTime() + " " + res_datetime + " " + res_datetime.getTime());      
            
            }else {
    //          print(" Results identical");
                res = true;}
            }
        else { //*/
            //assertEquals(testInt, resInt);
            
            if (table_type == "bool" && test_bool != res_bool)
                print("Results not identical on table type " + table_type + " " + test_bool + " " + res_bool);
            else if (table_type == "tinyint" && test_ubyte != res_ubyte) 
                print("Results not identical on table type " + table_type + " " + test_ubyte + " " + res_ubyte);       
            else if (table_type == "smallint" && test_short != res_short) 
                print("Results not identical on table type " + table_type + " " + test_short + " " + res_short);       
            else if (table_type == "int" && test_int != res_int) 
                print("Results not identical on table type " + table_type + " " + test_int + " " + res_int);       
            else if (table_type == "bigint" && test_long != res_long) 
                print("Results not identical on table type " + table_type + " " + test_long + " " + res_long);     
            else if (table_type == "real" && test_real != res_real) 
                print("Results not identical on table type " + table_type + " " + test_real + " " + res_real);     
            else if (table_type == "double" && test_double != res_double) 
                print("Results not identical on table type " + table_type + " " + test_double + " " + res_double);     
            else if (table_type == "varchar(100)" && !test_varchar.equals(res_varchar.trim()))  {
                print("Results not identical on table type " + table_type + " " + test_varchar + " " + res_varchar);       
                print(test_varchar.compareTo(res_varchar) + "a"+ test_varchar.length() + "b" + res_varchar.length());}
            else if (table_type == "date" && Math.abs(test_date.compareTo(res_date)) > 1) {
            //else if (table_type == "date" && !test_date.equals(res_date))  {  
                print("Results not identical on table type " + table_type + " " + test_date + " " + test_date.getTime() + " " + res_date + " " + res_date.getTime());      
                print(test_date.compareTo(res_date));}
            else if (table_type == "datetime" && !test_datetime.equals(res_datetime))
            //else if (table_type == "datetime" && Math.abs(test_datetime.compareTo(res_datetime)) > 1) 

                print("Results not identical on table type " + table_type + " " + test_datetime + " " + test_datetime.getTime() + " " + res_datetime + " " + res_datetime.getTime());      
            
            else {
                print(" Results identical");
                res = true;}
            
        //}
        
        
        
        return res;     
        
    }
    
    /*
    public boolean autoflush(int total_inserts, int insert_every) throws  IOException, SQLException, KeyManagementException, NoSuchAlgorithmException{
    
        boolean a_ok = false;
//      ConnectionHandle Client = new ConnectionHandle ("31.154.184.250", 5000, "sqream", "sqream", "master", false);
        ConnectionHandle Client = new ConnectionHandle (ConnectorTest.Host, ConnectorTest.Port, ConnectorTest.Usr, ConnectorTest.Pswd, ConnectorTest.DbName, ConnectorTest.Ssl, insert_every);

        
        Client = Client.connect();  
        // Client.setBulkRows(insert_every);
        //print("bulk size: " + Client.bulkinsert);

        // Prepare Table
        String table_type = "int";
        
        int multi_row_value = 8;
        //int row_num = 100000000;
        //print(" - Create Table t_" + table_type);
        String sql = MessageFormat.format("create or replace table t_{0} (x {0})", table_type);
        StatementHandle stmt = new StatementHandle(Client, sql); 
        stmt.prepare();     
        stmt.execute();
        stmt.close();
        
        //print(" - Insert " + table_type + " " + total_inserts + " times");
        sql = MessageFormat.format("insert into t_{0} values (?)", table_type);
        stmt = new StatementHandle(Client, sql); 
        stmt.prepare();
        stmt.execute(); 
        //* Insert a bunch of rows
        for(int i =0 ; i< total_inserts; i++) {   
          stmt.setInt(1,  multi_row_value);
          stmt.nextRow();  //  if ((i)%commitEvery==0)  
    
        }          
        stmt.close();

        return a_ok;
    }
    //*/ 
    
    public boolean get_tables_test() throws SQLException {
        conn = DriverManager.getConnection(url,"sqream","sqream");
        dbmeta = conn.getMetaData();
        // rs = dbmeta.getTables("master", "public", "test");
         //rs = dbmeta.getTables(null, null, "test", null);
        rs = dbmeta.getTables(null, null, null, null);
        // rsmeta = rs.getMetaData();
        while(rs.next()) 
            print(rs.getString(3));
        rs.close();
        conn.close();
        //conn.close();
        return true;
    }
    
/*
    public boolean check_nulls() throws SQLException {
        boolean a_ok = false;
        String table_name = "test_nulls"; 
        String sql = "create or replace table test_nulls (x tinyint)";

        conn = DriverManager.getConnection(url,"sqream","sqream");
        stmt = conn.createStatement();
        //sql = MessageFormat.format("create or replace table t_{0} (x {1})", table_name, table_type);
        stmt.execute(sql);
        stmt.close();

        
        // Insert value
//      
        sql = "insert into test_nulls values (?)";
        ps = conn.prepareStatement(sql);
        ps.setNull(1);
        ps.setBoolean(1, test_bool); 
        ps.addBatch();
        ps.executeBatch();
        ps.close();
        */      
    
    /*
    @Test
    public void insertBool() throws KeyManagementException, NoSuchAlgorithmException,  IOException, SQLException{
        new JDBC_Positive().insert("bool");
    }
    @Test
    public void insertTinyint() throws KeyManagementException, NoSuchAlgorithmException,  IOException, SQLException{
        new JDBC_Positive().insert("tinyint");
    }
    @Test
    public void insertSmallint() throws KeyManagementException, NoSuchAlgorithmException,  IOException, SQLException{
        new JDBC_Positive().insert("smallint");
    }
    @Test
    public void insertInt() throws KeyManagementException, NoSuchAlgorithmException,  IOException, SQLException{
        new JDBC_Positive().insert("int");
    }
    @Test
    public void insertBigint() throws KeyManagementException, NoSuchAlgorithmException,  IOException, SQLException{
        new JDBC_Positive().insert("bigint");
    }
    @Test
    public void insertReal() throws KeyManagementException, NoSuchAlgorithmException,  IOException, SQLException{
        new JDBC_Positive().insert("real");
    }
    @Test
    public void insertDouble() throws KeyManagementException, NoSuchAlgorithmException,  IOException, SQLException{
        new JDBC_Positive().insert("double");
    }
    @Test
    public void insertDatetime() throws KeyManagementException, NoSuchAlgorithmException,  IOException, SQLException{
        new JDBC_Positive().insert("datetime");
    }
    @Test
    public void insertDate() throws KeyManagementException, NoSuchAlgorithmException,  IOException, SQLException{
        new JDBC_Positive().insert("date");
    }
    @Test
    public void insertVarchar100() throws KeyManagementException, NoSuchAlgorithmException,  IOException, SQLException{
        new JDBC_Positive().insert("varchar(100)");
    }
    @Test
    public void insertnVarchar100() throws KeyManagementException, NoSuchAlgorithmException,  IOException, SQLException{
        new JDBC_Positive().insert("nvarchar(100)");
    }
    /*
     @Test
     public void autoFlush() throws KeyManagementException, NoSuchAlgorithmException,  IOException, SQLException{
         new JDBC_Positive().autoflush(10000, 100);
     }  
     */
    
    public static void main(String[] args) throws SQLException, KeyManagementException, NoSuchAlgorithmException, IOException, ClassNotFoundException, InterruptedException{
        
    	// Loading JDBC driver with a timezone test
    	ZoneId before_jdbc = ZoneId.systemDefault();
        Class.forName("com.sqream.jdbc.SQDriver");
        ZoneId after_jdbc = ZoneId.systemDefault();
        print ("Changing timezone test: " + ((after_jdbc.equals(before_jdbc)) ? "OK" : "Fail"));
        // boolean all_pass = false;
        
        JDBC_Positive pos_tests = new JDBC_Positive();
        String[] typelist = {"bool", "tinyint", "smallint", "int", "bigint", "real", "double", "varchar(100)", "nvarchar(100)", "date", "datetime"};
        //String[] typelist = {"varchar(100)", "nvarchar(100)"}; //"nvarchar(100)"
        
        //String[] typelist = {"bool", "tinyint", "smallint", "int", "bigint", "real", "double", "varchar(100)", "nvarchar(100)", "date", "datetime"};
        print ("Limited fetch test - " + (pos_tests.limited_fetch() ? "OK" : "Fail"));
        print ("Pre fetch test - " + (pos_tests.pre_fetch() ? "OK" : "Fail"));
        /*
        print ("Display size test - " + (pos_tests.display_size() ? "OK" : "Fail"));
        print ("parameter metadata test: " + (pos_tests.parameter_metadata() ? "OK" : "Fail"));
        print ("logging is off test:" + (pos_tests.is_logging_off() ? "OK" : "Fail"));
        print ("boolean as string test - " + (pos_tests.bool_as_string() ? "OK" : "Fail"));
        print ("Cast test - " + (pos_tests.casted_gets() ? "OK" : "Fail"));
        print ("timeZones test - " + (pos_tests.timeZones() ? "OK" : "Fail"));
        print ("getUDF test - " + (pos_tests.getUDF() ? "OK" : "Fail"));
        print ("isSigned test - " + (pos_tests.isSigned() ? "OK" : "Fail"));
        print ("Execute batch test - " + (pos_tests.execBatchRes() ? "OK" : "Fail"));
        //*
        for (String col_type : typelist)
            if(!pos_tests.insert(col_type))  
                throw new java.lang.RuntimeException("Not all type checks returned identical");
                
                // */
        
        //pos_tests.get_tables_test();
        /*
        try {
            pos_tests.autoflush(10000, 100);
        }catch (java.lang.ArrayIndexOutOfBoundsException e) {
            print("Correct error on overflowing buffer with addBatch()");
        }    */

    }  
}




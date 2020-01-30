package com.sqream.jdbc;

import java.text.MessageFormat;
import java.time.LocalTime;
import java.util.*;
//import org.junit.Assert;
//import org.junit.Test;
import java.util.logging.Logger;
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
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static com.sqream.jdbc.TestEnvironment.*;
import static java.sql.Types.*;
import static org.junit.Assert.*;

@RunWith(JUnit4.class)
public class JDBC_Positive {

    private static final Logger log = Logger.getLogger(JDBC_Positive.class.toString());

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
    String    res_nvarchar = "";
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
    static final String url = URL;
    
    Statement stmt = null;
    ResultSet rs = null;
    Connection conn  = null;
    Connection conn2 = null;
    PreparedStatement ps = null;
    
    // For testTables() test
    DatabaseMetaData dbmeta = null;
    ResultSetMetaData rsmeta = null;
    // Load JDBC driver

	
	static void printbuf(ByteBuffer to_print, String description) {
		log.info(description + " : " + to_print);
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
	
	@Test
	public void hundredMilFetch() throws SQLException {
        String createSql = "create or replace table test_fetch (ints int)";
        String insertSql = "insert into test_fetch values (?)";
        String selectSql = "select * from test_fetch";
        int randomInt = 8;
        int times = 100_000_000;  // Assuming chunk size is around 1 million, giving X10 more
        int fetchCounter = 0;

	    try (Connection conn = createConnection();
             Statement stmt = conn.createStatement()) {

            stmt.executeQuery(createSql);
        }
        try (Connection conn = createConnection();
             PreparedStatement ps = conn.prepareStatement(insertSql)) {
            for (int i = 0; i < times; i++) {
                ps.setInt(1, randomInt);
                ps.addBatch();
            }
            ps.executeBatch();
        }
        try (Connection conn = createConnection();
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(selectSql)) {
            while(rs.next()) {
                rs.getInt(1);
                fetchCounter++;
            }
        }
        assertEquals(fetchCounter, times);
	}
	
	@Test
	public void unusedFetchTest() throws SQLException {
        String sqlCreate = "create or replace table test_fetch (ints int)";
        String sqlInsert = "insert into test_fetch values (?)";
        String sqlSelectAll = "select * from test_fetch";
        String sqlSelectOne = "select 1";

		int maxRows = 3;

		try (Connection conn = DriverManager.getConnection(url,"sqream","sqream")) {

		    try (Statement stmt = conn.createStatement()) {
                stmt.execute(sqlCreate);
            }

            try (PreparedStatement ps = conn.prepareStatement(sqlInsert)) {
                int random_int = 8;
                int times = 10000000;  // Assuming chunk size is around 1 million, giving X10 more
                for (int i = 0; i < times; i++) {
                    ps.setInt(1, random_int);
                    ps.addBatch();
                }
                ps.executeBatch();
            }

            try (Statement stmt = conn.createStatement()) {
                stmt.setMaxRows(maxRows);
                ResultSet rs = stmt.executeQuery(sqlSelectAll);
            }

            try (Statement stmt = conn.createStatement()) {
                ResultSet rs = stmt.executeQuery(sqlSelectOne);
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
            }
        }
	}
	
	@Test
	public void displaySize() throws SQLException {
	    String createSql = "create or replace table test_display (x varchar(11))";
	    String selectSql = "select * from test_display";

        ResultSetMetaData rsmeta;
        try (Connection conn = createConnection();
             Statement stmt = conn.createStatement()) {

            stmt.execute(createSql);
            ResultSet rs = stmt.executeQuery(selectSql);
            rsmeta = rs.getMetaData();
            rs.close();
        }

        assertNotNull(rsmeta);
        assertEquals(11, rsmeta.getColumnDisplaySize(1));
    }

    @Test
    public void parameter_metadata() throws SQLException {
        /*  Check if charitable behavior works - not closing statement before starting the next one   */
        
   	 boolean a_ok = true;
        
        // Count test - DML
        conn = DriverManager.getConnection(url,"sqream","sqream");
        String sql = "create or replace table test_parameter(bools bool not null, tinies tinyint, smalls smallint, ints int, bigs bigint, floats real, doubles double, dates date, dts datetime, varcs varchar (10), nvarcs nvarchar (10))";
        ps = conn.prepareStatement(sql);
        ParameterMetaData params = ps.getParameterMetaData();
        int count = params.getParameterCount();
        if (count != 0) {
        	log.info("Should have 0 parameter count on a DML query, but got: " + count);
        	a_ok = false;
        }
    	ps.close();
       
    	// Count test - regular insert
        sql = "insert into test_parameter values (true, 1, 11, 111, 1111, 1.1, 1.11, '2016-11-03', '2016-11-03 16:56:45.000', 'bla', 'nbla')";
        ps = conn.prepareStatement(sql);
        params = ps.getParameterMetaData();
        count = params.getParameterCount() ;
        if (count != 0) {
        	log.info ("Should have 0 parameter count on a regular insert query, but got: " + count);
	        a_ok = false;
	    }
        ps.close();
        
        // Network insert - an actual paramtered query
        sql = "insert into test_parameter values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        ps = conn.prepareStatement(sql);
        params = ps.getParameterMetaData();
        
        count = params.getParameterCount() ;
        if (count != 11) {
        	log.info ("Should have 3 parameter count on a network insert, but got: " + count);
        	a_ok = false;
        }
        
    	if (!params.getParameterClassName(1).equals("denied") || !params.getParameterClassName(2).equals("denied") || 
			!params.getParameterClassName(3).equals("denied") || !params.getParameterClassName(4).equals("denied") || 
			!params.getParameterClassName(5).equals("denied") || !params.getParameterClassName(6).equals("denied") || 
			!params.getParameterClassName(7).equals("denied") || !params.getParameterClassName(8).equals("denied") || 
			!params.getParameterClassName(9).equals("denied") || !params.getParameterClassName(10).equals("denied") || 
			!params.getParameterClassName(11).equals("denied"))
    	{
    		log.info ("Bad column names returned:\n" + params.getParameterClassName(1) + '\n' + params.getParameterClassName(2) + '\n' + params.getParameterClassName(3) + '\n' + params.getParameterClassName(4) + '\n' + params.getParameterClassName(5) + '\n' + params.getParameterClassName(6) + '\n' + params.getParameterClassName(7) + '\n' + params.getParameterClassName(8) + '\n' + params.getParameterClassName(9) + '\n' + params.getParameterClassName(10) + '\n' + params.getParameterClassName(11)  );
        	a_ok = false;
    	}
    	
    	if (params.getParameterMode(1) != ParameterMetaData.parameterModeIn || params.getParameterMode(2) != ParameterMetaData.parameterModeIn  || 
			params.getParameterMode(3) != ParameterMetaData.parameterModeIn || params.getParameterMode(4) != ParameterMetaData.parameterModeIn  || 
			params.getParameterMode(5) != ParameterMetaData.parameterModeIn || params.getParameterMode(6) != ParameterMetaData.parameterModeIn  || 
			params.getParameterMode(7) != ParameterMetaData.parameterModeIn || params.getParameterMode(8) != ParameterMetaData.parameterModeIn  || 
			params.getParameterMode(9) != ParameterMetaData.parameterModeIn || params.getParameterMode(10) != ParameterMetaData.parameterModeIn  || 
			params.getParameterMode(11) != ParameterMetaData.parameterModeIn) 
    	{
    		log.info ("Bad parameter mode returned: " + params.getParameterMode(1));
        	a_ok = false;
    	}
    	
    	if (params.getScale(1) != 0 || params.getScale(2) != 0 || params.getScale(3) != 0 || 
			params.getScale(4) != 0 || params.getScale(5) != 0 || params.getScale(6) != 0 || 
			params.getScale(7) != 0 || params.getScale(8) != 0 || params.getScale(9) != 0 || 
			params.getScale(10) != 0 || params.getScale(11) != 0)
		{
    		// 4 on float, 8 on double, 0 elsewhere
    		log.info ("Bad scale returned: " + params.getScale(2));
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
    		log.info ("Bad isNullable returned: " + params.isNullable(1));
        	a_ok = false;
    	}
    	
    	if (params.getParameterType(1) != Types.BOOLEAN || params.getParameterType(2) != TINYINT ||
			params.getParameterType(3) != Types.SMALLINT || params.getParameterType(4) != Types.INTEGER || 
			params.getParameterType(5) != Types.BIGINT || params.getParameterType(6) != Types.REAL || 
			params.getParameterType(7) != Types.DOUBLE || params.getParameterType(8) != Types.DATE || 
			params.getParameterType(9) != Types.TIMESTAMP || params.getParameterType(10) != Types.VARCHAR || 
			params.getParameterType(11) != Types.NVARCHAR)
		{
    		log.info ("Bad parameter type returned: " + params.isNullable(1));
        	a_ok = false;
    	}
    	
    	//params.getParameterType(1)
    	if (params.getPrecision(1) != 1 || params.getPrecision(2) != 1 || params.getPrecision(3) != 2 || 
			params.getPrecision(4) != 4 || params.getPrecision(5) != 8 || params.getPrecision(6) != 4 || 
			params.getPrecision(7) != 8 || params.getPrecision(8) != 4 || params.getPrecision(9) != 8 ||
            params.getPrecision(10) != 10 || params.getPrecision(11) == 0)
		{
    		log.info ("Bad precision returned from parameter test:\n" + params.getPrecision(1) + '\n' + params.getPrecision(2) + '\n' + params.getPrecision(3) + '\n' + params.getPrecision(4) + '\n' + params.getPrecision(5) + '\n' + params.getPrecision(6) + '\n' + params.getPrecision(7) + '\n' + params.getPrecision(8) + '\n' + params.getPrecision(9) + '\n' + params.getPrecision(10) + '\n' + params.getPrecision(11)  );
        	a_ok = false;
    	}
    	
    	if (!params.getParameterTypeName(1).equals("ftBool") || !params.getParameterTypeName(2).equals("ftUByte") || 
			!params.getParameterTypeName(3).equals("ftShort") || !params.getParameterTypeName(4).equals("ftInt") || 
			!params.getParameterTypeName(5).equals("ftLong") || !params.getParameterTypeName(6).equals("ftFloat") || 
			!params.getParameterTypeName(7).equals("ftDouble") || !params.getParameterTypeName(8).equals("ftDate") || 
			!params.getParameterTypeName(9).equals("ftDateTime") || !params.getParameterTypeName(10).equals("ftVarchar") || 
			!params.getParameterTypeName(11).equals("ftBlob"))
    	{
    		log.info ("Bad taypenames returned:\n" + params.getParameterTypeName(1) + '\n' + params.getParameterTypeName(2) + '\n' + params.getParameterTypeName(3) + '\n' + params.getParameterTypeName(4) + '\n' + params.getParameterTypeName(5) + '\n' + params.getParameterTypeName(6) + '\n' + params.getParameterTypeName(7) + '\n' + params.getParameterTypeName(8) + '\n' + params.getParameterTypeName(9) + '\n' + params.getParameterTypeName(10) + '\n' + params.getParameterTypeName(11)  );
        	a_ok = false;
    	}
    	
    	//params.getParameterType(1)
    	if (params.isSigned(1) != false || params.isSigned(2) != false || params.isSigned(3) != true ||
			params.isSigned(4) != true || params.isSigned(5) != true || params.isSigned(6) != true ||		
			params.isSigned(7) != true || params.isSigned(8) != false || params.isSigned(9) != false ||
			params.isSigned(10) != false || params.isSigned(11) != false)
    	{
    		log.info ("Bad values returned on isSigned():" + params.isSigned(1));
        	a_ok = false;
    	}
    	
    	ps.close();

        assertTrue(a_ok);
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
    
    @Test
    public void boolAsString() throws SQLException {
        String sqlCreate = "create or replace table bool_string (x bool, y bool)";
        String sqlInsert = "insert into bool_string values (true, false)";
    	String sqlSelect = "select * from bool_string";

    	try (Connection conn = createConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute(sqlCreate);
            stmt.execute(sqlInsert);
            ResultSet rs = stmt.executeQuery(sqlSelect);

            assertTrue(rs.next());
            assertEquals("true", rs.getString(1));
            assertEquals("false", rs.getString(2));
        }
    }
    
    @Test
     public void getUDF() throws SQLException {
        String createSql = "CREATE OR REPLACE FUNCTION fud () RETURNS int as $$ return 1 $$ LANGUAGE PYTHON";

        try (Connection conn = createConnection();
             Statement stmt = conn.createStatement()) {

            stmt.execute(createSql);
            DatabaseMetaData metaData = conn.getMetaData();

            try (ResultSet rs = metaData.getProcedures(null, null, null)) {
                while(rs.next()) {
                    assertEquals("fud", rs.getString("procedure_name"));
                }
            }
        }
    }

    @Test
    public void isSigned() throws SQLException {
        String createSql = "create or replace table test_signed (x int, y varchar(10))";
        String selectSql = "select * from test_signed";

        try (Connection conn = createConnection();
             Statement stmt = conn.createStatement()) {

            stmt.execute(createSql);

            try (ResultSet rs = stmt.executeQuery(selectSql)) {
                ResultSetMetaData metaData = rs.getMetaData();

                assertTrue(metaData.isSigned(1));
                assertFalse(metaData.isSigned(2));
            }
        }
    }
    
    @Test
    public void timeZones() throws SQLException {
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
        log.info();("Originals - date: " +  date + " datetime: " + datetime);
        log.info();("Retrieved wo calendar - date: " + resDate + " datetime: " + resDateTime );
        log.info();("Retrieved with calendar- date: " + resDateZoned + " datetime: " + resDateTimeZoned );
        log.info();("Does equal date: " + (date.compareTo(resDateZoned)));
        log.info();("Does equal datetime: " + (datetime.compareTo(resDateTimeZoned)));
        log.info();("Does equal datetime when retreived with no cal: " + (datetime.compareTo(resDateTime)));
        //*/
        // log.info();("a_ok: " + a_ok);

        assertTrue(a_ok);
    }
    
    @Test
    public void casted_gets() throws SQLException {
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
        	log.info ("bad casted getInt on byte column");
        else if (test_byte != rs.getShort(1))
        	log.info ("bad casted getShort on byte column");
        else if (test_short != rs.getInt(2))
			log.info ("bad casted getInt on short column");
        else if (test_float != rs.getDouble(3))
			log.info ("bad casted getDouble on float column");
        else
        	a_ok = true;
        
        rs.close();
        stmt.close();

        assertTrue(a_ok);
    }

    @Test
    public void timezoneTest() throws ClassNotFoundException {
        // Loading JDBC driver with a timezone test
        ZoneId before_jdbc = ZoneId.systemDefault();
        Class.forName("com.sqream.jdbc.SQDriver");
        ZoneId after_jdbc = ZoneId.systemDefault();
        log.info ("Changing timezone test: " + ((after_jdbc.equals(before_jdbc)) ? "OK" : "Fail"));
    }

    @Test
    public void typesTest() throws NoSuchAlgorithmException, SQLException, KeyManagementException, IOException {
        String[] typelist = {"bool", "tinyint", "smallint", "int", "bigint", "real", "double", "varchar(100)", "nvarchar(100)", "date", "datetime"};

        for (String col_type : typelist) {
            insert(col_type);
        }
    }

    @Test
    public void setValuesAsNullTest() throws SQLException {
        String createSql = "create or replace table test_null_values " +
                "(bools bool, bytes tinyint, shorts smallint, ints int, bigints bigint, floats real, doubles double, " +
                "strings varchar(10), strangs nvarchar(10), dates date, dts datetime)";
        String insertSql = "insert into test_null_values values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);";
        try (Connection conn = DriverManager.getConnection(url, "sqream", "sqream");
             Statement stmt = conn.createStatement()) {
            stmt.execute(createSql);
        }

        try (Connection conn = DriverManager.getConnection(url, "sqream", "sqream");
             PreparedStatement ps = conn.prepareStatement(insertSql)) {
            ps.setNull(1, BOOLEAN);
            ps.setNull(2, TINYINT);
            ps.setNull(3,SMALLINT);
            ps.setNull(4,INTEGER);
            ps.setNull(5,BIGINT);
            ps.setNull(6,REAL);
            ps.setNull(7,DOUBLE);
            ps.setString(8,null);
            ps.setString(9, null);
            ps.setDate(10, null);
            ps.setTimestamp(11, null);
            ps.addBatch();
        }

        String selectSql = "select * from test_null_values";
        try (Connection conn = DriverManager.getConnection(url, "sqream", "sqream");
             Statement stmt = conn.createStatement()) {
            stmt.execute(selectSql);
            ResultSet rs = stmt.getResultSet();

            assertTrue(rs.next());
            assertNull(rs.getObject(1));
            assertNull(rs.getObject(2));
            assertNull(rs.getObject(3));
            assertNull(rs.getObject(4));
            assertNull(rs.getObject(5));
            assertNull(rs.getObject(6));
            assertNull(rs.getObject(7));
            assertNull(rs.getString(8));
            assertNull(rs.getString(9));
            assertNull(rs.getDate(10));
            assertNull(rs.getTimestamp(11));
        }
    }


    @Test(expected = SQLException.class)
    public void bad_message() throws SQLException {
        String sql = "select momo";
        String expectedErrorMessage = "At row 1, col 8: identifier not found momo";

	    try (Connection conn = DriverManager.getConnection(url,"sqream","sqream");
             Statement stmt = conn.createStatement()) {
            stmt.executeQuery(sql);
        } catch (SQLException e) {
	        if (expectedErrorMessage.equals(e.getMessage().substring(0, expectedErrorMessage.length()))) {
	            throw e;
            } else {
                fail(MessageFormat.format("Wrong exception message: [{0}]", e.getMessage()));
            }
        }
    }

    @Test
    public void stores_supports_functions() throws SQLException {
	    try (Connection conn = DriverManager.getConnection(url,"sqream","sqream")) {
            DatabaseMetaData dbmeta = conn.getMetaData();

            assertTrue(dbmeta.storesLowerCaseIdentifiers());
            assertTrue(dbmeta.storesMixedCaseQuotedIdentifiers());
            assertTrue(dbmeta.supportsMixedCaseQuotedIdentifiers());
        }
    }

    @Test
    public void get_x_functions() throws SQLException {
        String expectedTimeDateFunctions = "curdate, curtime, dayname, dayofmonth, dayofweek, dayofyear, hour, minute, month, monthname, now, quarter, timestampadd, timestampdiff, second, week, year";
	    String expectedStringFunctions = "CHAR_LENGTH, CHARINDEX, ||, ISPREFIXOF, LEFT, LEN, LIKE, LOWER, LTRIM, OCTET_LENGTH, PATINDEX, REGEXP_COUNT, REGEXP_INSTR, REGEXP_SUBSTR, REPLACE, REVERSE, RIGHT, RLIKE, RTRIM, SUBSTRING, TRIM, LOWER";
        String expectedSystemFunctions = "explain, show_connections, show_locks, show_node_info, show_server_status, show_version, stop_statement";
        String expectedNumericFunctions = "ABS, ACOS, ASIN, ATAN, ATN2, CEILING, CEIL, COS, COT, CRC64, DEGREES, EXP, FLOOR, LOG, LOG10, MOD, %, PI, POWER, RADIANS, ROUND, SIN, SQRT, SQUARE, TAN, TRUNC";

        try (Connection conn = DriverManager.getConnection(url,"sqream","sqream")) {
            DatabaseMetaData dbmeta = conn.getMetaData();

            assertEquals(expectedTimeDateFunctions, dbmeta.getTimeDateFunctions());
            assertEquals(expectedStringFunctions, dbmeta.getStringFunctions());
            assertEquals(expectedSystemFunctions, dbmeta.getSystemFunctions());
            assertEquals(expectedNumericFunctions, dbmeta.getNumericFunctions());
        }
    }

    @Test
    public void sqreamHang2Test() throws SQLException {

        try (Connection conn = createConnection();
             Statement stmt = conn.createStatement()) {

            stmt.execute("CREATE OR REPLACE TABLE public.class (Name VARCHAR(8),Sex VARCHAR(1),Age DOUBLE,Height DOUBLE, Weight DOUBLE);");
        }

        try (Connection conn0 = createConnection();
             Statement stmt0 = conn0.createStatement()) {

            stmt0.setQueryTimeout(0);
            stmt0.execute("SELECT * FROM public.CLASS WHERE 0=1");

            try (Connection conn1 = createConnection();
                 Statement stmt1 = conn0.createStatement()) {

                stmt1.execute("SELECT * FROM public.CLASS WHERE 0=1");

                try (Connection conn2 = createConnection();
                     Statement stmt2 = conn0.createStatement()) {

                    stmt2.execute("CREATE OR REPLACE TABLE public.class22 (Name VARCHAR(8),Sex VARCHAR(1),Age DOUBLE,Height DOUBLE, Weight DOUBLE)");
                }
            }
        }
    }


    private void insert(String table_type) throws IOException, SQLException {

        String table_name = table_type.contains("varchar(100)") ?  table_type.substring(0,7) : table_type;
        table_name = table_name.toUpperCase();
        String sql;

        conn = DriverManager.getConnection(url,"sqream","sqream");

        // Prepare Table
//      log.info();(" - Create Table t_" + table_type);
        stmt = conn.createStatement();
        sql = MessageFormat.format("create or replace table t_{0} (Xx {1})", table_name, table_type);
        stmt.execute(sql);
        if (stmt != null){
            stmt.close();
        }

        // Insert value
        sql = MessageFormat.format("insert into t_{0} values (?)", table_name);
        if ("bool".equals(table_type)) {
            for (boolean expectedBool : test_bools) {
                ps = conn.prepareStatement(sql);
                ps.setBoolean(1, expectedBool);
                send_and_retreive_result(ps, table_name, table_type);

                assertEquals(expectedBool, res_bool);
            }
        } else if ("tinyint".equals(table_type)) {
            for (byte excpectedUbyte : test_ubytes) {
                ps = conn.prepareStatement(sql);
                ps.setByte(1, excpectedUbyte);
                send_and_retreive_result(ps, table_name, table_type);

                assertEquals(excpectedUbyte, res_ubyte);
            }
        } else if ("smallint".equals(table_type)) {
            for (short expectedShort : test_shorts) {
                ps = conn.prepareStatement(sql);
                ps.setShort(1, expectedShort);
                send_and_retreive_result(ps, table_name, table_type);

                assertEquals(expectedShort, res_short);
            }
        }
        else if ("int".equals(table_type)) {
            for (int expectedInt : test_ints) {
                ps = conn.prepareStatement(sql);
                ps.setInt(1, expectedInt);
                send_and_retreive_result(ps, table_name, table_type);

                assertEquals(expectedInt, res_int);
            }
        } else if ("bigint".equals(table_type)) {
            for (long expectedLong : test_longs) {
                ps = conn.prepareStatement(sql);
                ps.setLong(1, expectedLong);
                send_and_retreive_result(ps, table_name, table_type);

                assertEquals(expectedLong, res_long);
            }
        } else if ("real".equals(table_type)) {
            for (float expectedReal : test_reals) {
                ps = conn.prepareStatement(sql);
                ps.setFloat(1, expectedReal);
                send_and_retreive_result(ps, table_name, table_type);

                assertEquals(expectedReal, res_real, 0f);
            }
        } else if ("double".equals(table_type)) {
            for (double expectedDouble : test_doubles) {
                ps = conn.prepareStatement(sql);
                ps.setDouble(1, expectedDouble);
                send_and_retreive_result(ps, table_name, table_type);

                assertEquals(expectedDouble, res_double, 0d);
            }
        } else if ("varchar(100)".equals(table_type)) {
            for (String expectedVarchar : test_varchars) {
                ps = conn.prepareStatement(sql);
                ps.setString(1, expectedVarchar);
                send_and_retreive_result (ps, table_name, table_type);

                assertEquals(expectedVarchar, res_varchar.trim());
            }
        } else if ("nvarchar(100)".equals(table_type)) {
            for (String expectedNvarchar : test_varchars) {
                ps = conn.prepareStatement(sql);
                ps.setString(1, expectedNvarchar);
                send_and_retreive_result(ps, table_name, table_type);

                assertEquals(expectedNvarchar, res_nvarchar);
            }
        } else if ("date".equals(table_type)) {
            for (Date expectedDate : test_dates) {
                ps = conn.prepareStatement(sql);
                ps.setDate(1, expectedDate);
                send_and_retreive_result(ps, table_name, table_type);

                assertTrue(Math.abs(test_date.compareTo(res_date)) <= 1);
            }
        } else if ("datetime".equals(table_type)) {
            for (Timestamp expectedDatetime : test_datetimes) {
                ps = conn.prepareStatement(sql);
                ps.setTimestamp(1, expectedDatetime);
                send_and_retreive_result(ps, table_name, table_type);

                assertEquals(expectedDatetime, res_datetime);
            }
        }
    }
    
    public void send_and_retreive_result (PreparedStatement ps, String table_name, String table_type) throws IOException, SQLException {
        
        ps.addBatch();
        ps.executeBatch();
        
        ps.close();
        
        //*
        // Retreive
//      log.info();(" - Getting " + table_type + " value back for value");
        String sql = MessageFormat.format("select * from t_{0}", table_name);
        stmt = conn.createStatement();
        rs = stmt.executeQuery(sql);
        while(rs.next())
        {
            if (table_type == "bool") {
            	if (rs.getBoolean(1) != rs.getBoolean("Xx")) 
            		log.info ("Different results on getBoolean on index vs column name");
            	if (!rs.getString("Xx").equals(String.valueOf(rs.getBoolean("Xx"))))
            		log.info ("Different results on stringified getBoolean vs getString");
            	res_bool = rs.getBoolean(1);
            }else if (table_type == "tinyint") {
            	if (rs.getByte(1) != rs.getByte("Xx")) 
            		log.info ("Different results on getByte on index vs column name");
            	if (!rs.getString("Xx").equals(String.valueOf(rs.getByte("Xx")))) 
            		log.info ("Different results on stringified getByte vs getString");
            	if ((short) rs.getByte(1) != rs.getShort(1)) 
            		log.info ("Different results on getByte vs getShort");
            	if ((int) rs.getByte(1) != rs.getInt(1)) 
            		log.info ("Different results on getByte vs getInt");
            	if ((long) rs.getByte(1) != rs.getLong(1)) 
            		log.info ("Different results on getByte vs getLong");
            	if ((float) rs.getByte(1) != rs.getFloat(1)) 
            		log.info ("Different results on getByte vs getFloat");
            	if ((double) rs.getByte(1) != rs.getDouble(1)) 
            		log.info ("Different results on getByte vs getDouble");
                res_ubyte = rs.getByte(1);
            }else if (table_type == "smallint") {
            	if (rs.getShort(1) != rs.getShort("Xx")) 
            		log.info ("Different results on getShort on index vs column name");
            	if (!rs.getString("Xx").equals(String.valueOf(rs.getShort("Xx")))) 
            		log.info ("Different results on stringified getShort vs getString");
            	if ((int) rs.getShort(1) != rs.getInt(1)) 
            		log.info ("Different results on getShort vs getInt");
            	if ((long) rs.getShort(1) != rs.getLong(1)) 
            		log.info ("Different results on getShort vs getLong");
            	if ((float) rs.getShort(1) != rs.getFloat(1)) 
            		log.info ("Different results on getShort vs getFloat");
            	if ((double) rs.getShort(1) != rs.getDouble(1)) 
            		log.info ("Different results on getShort vs getDouble");
            	res_short = rs.getShort(1);
            }else if (table_type == "int") {
            	if (rs.getInt(1) != rs.getInt("Xx")) 
            		log.info ("Different results on getInt on index vs column name");
            	if (!rs.getString("Xx").equals(String.valueOf(rs.getInt("Xx")))) 
            		log.info ("Different results on stringified getInt vs getString");
            	if ((long) rs.getInt(1) != rs.getLong(1)) 
            		log.info ("Different results on getInt vs getLong");
            	if ((float) rs.getInt(1) != rs.getFloat(1)) 
            		log.info ("Different results on getInt vs getFloat");
            	if ((double) rs.getInt(1) != rs.getDouble(1)) 
            		log.info ("Different results on getInt vs getDouble");
            	res_int = rs.getInt(1);
            }else if (table_type == "bigint") {
            	if (rs.getLong(1) != rs.getLong("Xx")) 
            		log.info ("Different results on getLong on index vs column name");
            	if (!rs.getString("Xx").equals(String.valueOf(rs.getLong("Xx")))) 
            		log.info ("Different results on stringified getLong vs getString");
            	if ((double) rs.getLong(1) != rs.getDouble(1)) 
            		log.info ("Different results on getLong vs getDouble");
            	res_long = rs.getLong(1);
            }else if (table_type == "real") {
            	if (rs.getFloat(1) != rs.getFloat("Xx")) 
            		log.info ("Different results on getFloat on index vs column name");
            	if (!rs.getString("Xx").equals(String.valueOf(rs.getFloat("Xx")))) 
            		log.info ("Different results on stringified getFloat vs getString");
            	if ((double) rs.getFloat(1) != rs.getDouble(1)) 
            		log.info ("Different results on getFloat vs getDouble");
            	res_real = rs.getFloat(1);
            }else if (table_type == "double") {
            	if (rs.getDouble(1) != rs.getDouble("Xx"))
            		log.info ("Different results on getDouble on index vs column name");
            	if (!rs.getString("Xx").equals(String.valueOf(rs.getDouble("Xx")))) 
            		log.info ("Different results on stringified getDouble vs getString");
                res_double = rs.getDouble(1);
            }else if (table_type == "varchar(100)") {
            	if (!rs.getString(1).equals(rs.getString("Xx")))
            		log.info ("Different results on getString on index vs column name");
                res_varchar = rs.getString(1);
            }else if (table_type == "nvarchar(100)") {
            	if (!rs.getString(1).equals(rs.getString("Xx")))
            		log.info ("Different results on getString on index vs column name");
                res_nvarchar = rs.getString(1);
            }else if (table_type == "date") {
            	if (Math.abs(rs.getDate(1).compareTo(rs.getDate("Xx"))) > 1) 
            		log.info ("Different results on getDate on index vs column name");
            	if (!rs.getString("Xx").equals(String.valueOf(rs.getDate("Xx")))) 
            		log.info ("Different results on stringified getDate vs getString");
                res_date = rs.getDate(1);
            }else if (table_type == "datetime") {
            	if (Math.abs(rs.getTimestamp(1).compareTo(rs.getTimestamp("Xx"))) > 1) 
            		log.info ("Different results on getTimestamp on index vs column name");
            	if (!rs.getString("Xx").equals(String.valueOf(rs.getTimestamp("Xx")))) 
            		log.info ("Different results on stringified getTimestamp vs getString");
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
    
    public boolean get_tables_test() throws SQLException {
        conn = DriverManager.getConnection(url,"sqream","sqream");
        dbmeta = conn.getMetaData();
        // rs = dbmeta.getTables("master", "public", "test");
         //rs = dbmeta.getTables(null, null, "test", null);
        rs = dbmeta.getTables(null, null, null, null);
        // rsmeta = rs.getMetaData();
        while(rs.next()) 
            log.info(rs.getString(3));
        rs.close();
        conn.close();
        //conn.close();
        return true;
    }

    private Connection createConnection() {
        try {
            return DriverManager.getConnection(url,"sqream","sqream");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
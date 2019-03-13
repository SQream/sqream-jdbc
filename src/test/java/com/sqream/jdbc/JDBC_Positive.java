package com.sqream.jdbc;

import java.text.MessageFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Random;
import java.util.UUID;

import org.junit.Assert;
import org.junit.Test;
import java.util.stream.IntStream;

import java.sql.Date;
import java.sql.Timestamp;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.PreparedStatement;
import java.sql.DatabaseMetaData;    // for getTables test
import java.sql.ResultSetMetaData;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;


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
    static final String url = "jdbc:Sqream://127.0.0.1:5000/master;user=sqream;password=sqream;cluster=false;ssl=false";
    
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
		System.out.println(description + " : " + to_print);
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
            udfName = rs.getString("PROCEDURE_NAME"); 

        System.out.println("udf name: " + udfName);
        
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
    
    
    public boolean insert(String table_type) throws IOException, SQLException, KeyManagementException, NoSuchAlgorithmException{
        
        boolean a_ok = false;
        String table_name = table_type.contains("varchar(100)") ?  table_type.substring(0,7) : table_type;
        String sql;

        conn = DriverManager.getConnection(url,"sqream","sqream");
        
        // Prepare Table
//      System.out.println(" - Create Table t_" + table_type);
        stmt = conn.createStatement();
        sql = MessageFormat.format("create or replace table t_{0} (x {1})", table_name, table_type);
        stmt.execute(sql);
        if (stmt != null){
            stmt.close();
        }
        
        // Insert value
//      System.out.println(" - Insert test value " + table_type);
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
                //System.out.println("datetime: " + test_datetime);
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
//      System.out.println(" - Getting " + table_type + " value back for value");
        String sql = MessageFormat.format("select * from t_{0}", table_name);
        stmt = conn.createStatement();
        rs = stmt.executeQuery(sql);
        while(rs.next())
        {
            if (table_type == "bool") 
                res_bool = rs.getBoolean(1);
            else if (table_type == "tinyint") 
                res_ubyte = rs.getByte(1);
            else if (table_type == "smallint") 
                res_short = rs.getShort(1);
            else if (table_type == "int") 
                res_int = rs.getInt(1);
            else if (table_type == "bigint")
                res_long = rs.getLong(1);
            else if (table_type == "real")
                res_real = rs.getFloat(1);
            else if (table_type == "double")
                res_double = rs.getDouble(1);
            else if (table_type == "varchar(100)")
                res_varchar = rs.getString(1);
            else if (table_type == "nvarchar(100)")
                res_varchar = rs.getString(1);
            else if (table_type == "date")
                res_date = rs.getDate(1);
            else if (table_type == "datetime")
                res_datetime = rs.getTimestamp(1); 
        }  //*/ 
        rs.close();
        stmt.close();
    }
    
    public boolean is_identical(String table_type) {
        
        boolean use_junit = false;

        boolean res = false;
        //assertEquals(test_int, res_int);
        if (use_junit) {
            if (table_type == "bool") {
                Assert.assertEquals(test_bool, res_bool);
    //          System.out.println("Results not identical on table type " + table_type + " " + test_bool + " " + res_bool);
            }else if (table_type == "tinyint") {
                Assert.assertEquals(test_ubyte, res_ubyte);
    //          System.out.println("Results not identical on table type " + table_type + " " + test_ubyte + " " + res_ubyte);       
            }else if (table_type == "smallint") {
                Assert.assertEquals(test_short, res_short) ;
    //          System.out.println("Results not identical on table type " + table_type + " " + test_short + " " + res_short);       
            }else if (table_type == "int") {
                Assert.assertEquals(test_int, res_int); 
    //          System.out.println("Results not identical on table type " + table_type + " " + test_int + " " + res_int);       
            }else if (table_type == "bigint") {
                Assert.assertEquals(test_long, res_long) ;
    //          System.out.println("Results not identical on table type " + table_type + " " + test_long + " " + res_long);     
            }else if (table_type == "real") {
                Assert.assertEquals(test_real, res_real, 0.0) ;
    //          System.out.println("Results not identical on table type " + table_type + " " + test_real + " " + res_real);     
            }else if (table_type == "double") {
                Assert.assertEquals(test_double, res_double, 0.0) ;
    //          System.out.println("Results not identical on table type " + table_type + " " + test_double + " " + res_double);     
            }else if (table_type == "varchar(100)") {
                Assert.assertTrue(test_varchar.equals(res_varchar.trim())) ;
    //                  {
    //          System.out.println("Results not identical on table type " + table_type + " " + test_varchar + " " + res_varchar);       
    //          System.out.println(test_varchar.compareTo(res_varchar) + "a"+ test_varchar.length() + "b" + res_varchar.length());}
            }else if (table_type == "date") {
                    Assert.assertEquals(1, Math.abs(test_date.compareTo(res_date))) ;
    //      {
    //      //else if (table_type == "date" && !test_date.equals(res_date))  {  
    //          System.out.println("Results not identical on table type " + table_type + " " + test_date + " " + test_date.getTime() + " " + res_date + " " + res_date.getTime());      
    //          System.out.println(test_date.compareTo(res_date));}
            }else if (table_type == "datetime") {
                Assert.assertTrue(test_datetime.equals(res_datetime));
            //else if (table_type == "datetime" && Math.abs(test_datetime.compareTo(res_datetime)) > 1) 
    
    //          System.out.println("Results not identical on table type " + table_type + " " + test_datetime + " " + test_datetime.getTime() + " " + res_datetime + " " + res_datetime.getTime());      
            
            }else {
    //          System.out.println(" Results identical");
                res = true;}
            }
        else {
            //assertEquals(testInt, resInt);
            
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
            
        }
        
        
        return res;     
        
    }
    
    /*
    public boolean autoflush(int total_inserts, int insert_every) throws  IOException, SQLException, KeyManagementException, NoSuchAlgorithmException{
    
        boolean a_ok = false;
//      ConnectionHandle Client = new ConnectionHandle ("31.154.184.250", 5000, "sqream", "sqream", "master", false);
        ConnectionHandle Client = new ConnectionHandle (ConnectorTest.Host, ConnectorTest.Port, ConnectorTest.Usr, ConnectorTest.Pswd, ConnectorTest.DbName, ConnectorTest.Ssl, insert_every);

        
        Client = Client.connect();  
        // Client.setBulkRows(insert_every);
        //System.out.println("bulk size: " + Client.bulkinsert);

        // Prepare Table
        String table_type = "int";
        
        int multi_row_value = 8;
        //int row_num = 100000000;
        //System.out.println(" - Create Table t_" + table_type);
        String sql = MessageFormat.format("create or replace table t_{0} (x {0})", table_type);
        StatementHandle stmt = new StatementHandle(Client, sql); 
        stmt.prepare();     
        stmt.execute();
        stmt.close();
        
        //System.out.println(" - Insert " + table_type + " " + total_inserts + " times");
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
            System.out.println(rs.getString(3));
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
    
    //*
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
    
    public static void main(String[] args) throws SQLException, KeyManagementException, NoSuchAlgorithmException, IOException, ClassNotFoundException{
        
        Class.forName("com.sqream.jdbc.SQDriver");
        
        JDBC_Positive pos_tests = new JDBC_Positive();
        String[] typelist = {"bool", "tinyint", "smallint", "int", "bigint", "real", "double", "varchar(100)", "nvarchar(100)", "date", "datetime"};
        //String[] typelist = {"bool", "tinyint", "smallint", "int", "bigint", "real", "double", "varchar(100)", "nvarchar(100)", "date", "datetime"};

        if (pos_tests.getUDF()) 
            System.out.println("getUDF() test  - OK");
        else
            System.out.println("getUDF() test  - Fail");


        if (pos_tests.isSigned()) 
            System.out.println("isSigned() test  - OK");
        else
            System.out.println("isSigned() test  - Fail");
        
           
        if (pos_tests.execBatchRes()) 
            System.out.println("Execute batch reutrn value test  - OK");
        else
            System.out.println("Execute batch reutrn value test  - Fail");
        
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
            System.out.println("Correct error on overflowing buffer with addBatch()");
        }    */

    }  
}




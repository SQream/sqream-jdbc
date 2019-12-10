package com.sqream.jdbc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.MessageFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.HashMap;
import java.util.Map;

import javax.script.Bindings;
import javax.script.Invocable;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import java.sql.PreparedStatement;
import java.sql.ResultSet;


import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;

public class Perf {
    
    // Replace with your respective URL
    //static final String url_dst = "jdbc:Sqream://192.168.0.223:5000/master;user=sqream;password=sqream;cluster=false;ssl=false";
    //here sonoo is database name, root is username and password  
    
    Connection mysql_con;
    Connection conn  = null;
    Statement stmt = null;
    ResultSet rs = null;
    DatabaseMetaData dbmeta = null;
    PreparedStatement ps = null;
    String sql;
    
    int res = 0;
   
    static void print(Object printable) {
        System.out.println(printable);
    }
    
    static String format(String pattern, String value) {
    	
    	return MessageFormat.format(pattern, value);
    	
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

    public void perf(String url_src) throws SQLException, IOException {
        
        conn = DriverManager.getConnection(url_src,"sqream","sqream");
        //mysql_con=DriverManager.getConnection("jdbc:mysql://192.168.0.219:3306/perf","eliy","bladerfuK~1");  
        String sql;
        /*
        sql = "insert into perf_t2 values (?, ?)";
        ps = mysql_con.prepareStatement(sql);
        print ("before network insert");
        for(int i=1; i < 100000000; i++) {
            ps.setInt(1, 6);
            ps.setInt(2, 8);
            ps.addBatch();
            //ps.executeUpdate();m
            if (i % 10000 == 0) {
                print ("added batch number " + i);
                //ps.executeBatch();
            }
        }
        print ("after loop");
        //ps.executeBatch();  // Should be done automatically
        ps.close();
        print ("after network insert");
        //*/
        
        //*
        // create table
        sql = "create or replace table perf (bools bool, bytes tinyint, shorts smallint, ints int, bigints bigint, floats real, doubles double, strings varchar(10), strangs nvarchar(10))"; //, dates date, dts datetime)";

        stmt = conn.createStatement();
        stmt.execute(sql);
        stmt.close();
        
        // Network insert 10 million rows
        int amount = (int)Math.pow(10, 7);
        long start = time();
        sql = "insert into perf values (?, ?, ?, ?, ?, ?, ?, ?, ?)";
        ps = conn.prepareStatement(sql);
        
        for (int i=0; i < amount; i++) {
            ps.setBoolean(1, true);
            ps.setByte(2, (byte)120);
            ps.setShort(3, (short) 1400);
            ps.setInt(4, 140000);
            ps.setLong(5, (long) 5);
            ps.setFloat(6, (float)56.0);
            ps.setDouble(7, 57.0);
            ps.setString(8, "bla");
            ps.setString(9, "bla2");
            // ps.setDate(10, date_from_tuple(2019, 11, 26));
            // ps.setTimestamp(11, datetime_from_tuple(2019, 11, 26, 16, 45, 23, 45));
            ps.addBatch();
        }
        ps.executeBatch();  // Should be done automatically
        ps.close();

        print ("total network insert: " + (time() -start));
        
        // Check amount inserted
        sql = "select count(*) from perf";
        stmt = conn.createStatement();
        rs = stmt.executeQuery(sql);
        while(rs.next()) 
            print("row count: " + rs.getLong(1));
        rs.close();
        stmt.close();
        //*/
      
        /*
        sql = "create or replace table dt (dt datetime)";
        stmt = conn.createStatement();
        stmt.execute(sql);
        stmt.close();
        
        // Network insert 10 million rows
        sql = "insert into dt values (?)";
        ps = conn.prepareStatement(sql);
        Timestamp test = datetime_from_tuple(2018, 3, 23, 3, 54, 38, 0);
        print (test);
        
        for (int i=0; i < 1; i++) {
            ps.setTimestamp(1, test);
            ps.addBatch();
        }
        ps.executeBatch();  // Should be done automatically
        ps.close();

        
        // Check amount inserted
        sql = "select * from dt";
        stmt = conn.createStatement();
        rs = stmt.executeQuery(sql);
        while(rs.next()) 
            print(rs.getTimestamp(1));
        rs.close();
        stmt.close();
        //*/
        
      /*
        sql = "create or replace table excape (s varchar(50))";
        stmt = conn.createStatement();
        stmt.execute(sql);
        stmt.close();
        
        /*
        // Network insert 10 million rows
        sql = "insert into excape values (\"bla bla\")";
        ps = conn.prepareStatement(sql);
        ps.executeBatch();  // Should be done automatically
        ps.close();
		//*/
        
        /*  
        // Check amount inserted
        // sql = "select case when xint2%2=0 then xdate else '2015-01-01' end from t_a";
        StringBuilder s_sql = new StringBuilder("create or replace table t_test(x0 int");
        for(int i = 0; i < 59; i++)
        {
        	s_sql.append(",x" + (i+1) + " int");
        }
        s_sql.append(")");
        stmt = conn.createStatement();
        rs = stmt.executeQuery(s_sql.toString());
        while(rs.next()) 
            print(rs.getInt(1));
        rs.close();
        stmt.close();
        
        StringBuilder ss_sql = new StringBuilder("insert into t_test values(?");
        for(int i = 0; i < 59; i++)
        {
        	ss_sql.append(",?");
        }
        ss_sql.append(")");
        PreparedStatement p_stmt = conn.prepareStatement(ss_sql.toString());
        for(int i=0; i < 10; i++)
        {
        	for(int j = 0; j < 60; j++)
        	{
        		p_stmt.setInt(j+1, 11);
        	}
        	p_stmt.addBatch();
        }
        p_stmt.executeBatch();
        p_stmt.close();
        
        //conn.close();
        //*/
        
        
    	//*/
    }     
    
    public static void main(String[] args) throws SQLException, KeyManagementException, NoSuchAlgorithmException, IOException, ClassNotFoundException, ScriptException, NoSuchMethodException{
        
        // Load JDBC driver - not needed with newer version
        Class.forName("com.sqream.jdbc.SQDriver");
        //Class.forName("com.mysql.jdbc.Driver");  
        String url_src = "jdbc:Sqream://192.168.1.4:5000/master;user=sqream;password=sqream;cluster=false;ssl=false";
        //String url_src = "jdbc:Sqream://192.168.1.4:3108/master;user=sqream;password=sqream;cluster=true;service=sqream";

        
        Perf test = new Perf();   
        test.perf(url_src);
        
        /*
        //  --------
        String message = "'{'\"bla\":\"bla\"'}'"; 
        ScriptEngine engine = new ScriptEngineManager().getEngineByName("javascript");
        
        Invocable inv = (Invocable) engine;
        String message = "'{'\"bla\":\"bla\"'}'"; 
        
        //engine.put("input", new String(sample));
        String parse = "JSON.parse({0});";
        engine.put("input", message);
        String stringify = "JSON.stringify({0});";

        // Object inputjson = engine.eval(MessageFormat.format(parse, message));
        Object inputjson = engine.eval("JSON.parse(input);");

 
        print(inputjson);
        
        //print(JSONFunctions.parse((Object)message, (Object)message));
        
        //JSONParser parser = new JSONParser(message, Context.getGlobal(), false);        
        //JSONObject(message).toString();
        //print(parser.parse());
        // Map<String, String> map1 = (Map<String, Object>)engine.eval(
        //"JSON.parse('{ \"x\": 343, \"y\": \"hello\", \"z\": [2,4,5] }');");
        //print(map1);
         Map<String, String> prep = new HashMap<>();
         prep.put("prepareStatement", "insert into excape values (\"bla bla\")");
         
        ScriptEngine engine = new ScriptEngineManager().getEngineByName("nashorn");
        ScriptObjectMirror json = (ScriptObjectMirror) engine.eval("JSON");
        engine.getContext().getBindings(ScriptContext.GLOBAL_SCOPE).put ("statement", "insert into excape values (\"bla \n bla\")");

        
        //String stmt = "{\"prepareStatement\":\"insert into excape values (\\\"bla \\n bla\\\")\", \"chunkSize\":0}";
        //Bindings bindings = engine.createBindings();
        //bindings.put("prepareStatement", "insert into excape values (\"bla bla\")");
        //Object bindingsResult = engine.eval("JSON.parse(prepareStatement)", bindings);
        
        //Object parsed = json.callMember("parse", stmt);

        //Object parsed =  json.callMember("stringify", prep);
        //Object obj = engine.eval("var j = Java.asJSONCompatible({ prepareStatement: 'insert into excape values (\"bla bla\")'}); JSON.stringify(j);");
        String parsed = (String) engine.eval("var prop = {prepareStatement: statement}; JSON.stringify(prop)");

        //Map<String, Object> map = (Map<String, Object>)obj;
        //print (bindingsResult);
        //String map_maker = "var prep = {prepareStatement: \"insert into excape values (\"bla bla\")\")};" + 
      //                 "JSON.Stringify(preps);";
        
        print (parsed);
        //*/
    }
}



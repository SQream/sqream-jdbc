package com.sqream.jdbc;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;

import javax.script.ScriptException;

import org.junit.BeforeClass;
import org.junit.Test;

import com.sqream.jdbc.Connector;
import com.sqream.jdbc.Connector.ConnException;



public class Batch {
	static int bulkInsert = 10000;
	
	
	@BeforeClass
	public static void setup() throws IOException, ConnException, KeyManagementException, NoSuchAlgorithmException, SQLException, ScriptException{
		Connector conn = new Connector("127.0.0.1", 5000, false, false);
		conn.connect("master", "sqream", "sqream", "sqream");
		String sql = "create or replace table integer_string (x BIGINT, y VARCHAR(100))";		
		conn.execute(sql);
		conn.close();
		conn.close_connection(); 

	}
	@Test
	public void integerInsert() throws IOException, SQLException, KeyManagementException, NoSuchAlgorithmException, ScriptException, ConnException {
		Connector conn = new Connector("127.0.0.1", 5000, false, false);
		conn.connect("master", "sqream", "sqream", "sqream");
		String sql = String.format("insert into integer_string (x) values (?)");
		conn.set_long(1, (long)2008); 
		conn.execute(sql);
		conn.close();
		conn.close_connection();
	}
	@Test
	public void stringInsert()throws IOException, SQLException, KeyManagementException, NoSuchAlgorithmException, ConnException, ScriptException{
		Connector conn = new Connector("127.0.0.1", 5000, false, false);
		conn.connect("master", "sqream", "sqream", "sqream");
		String sql = String.format("insert into integer_string (y) values (?)");
		conn.set_varchar(1, "catsdogsx2catsdogsx2catsdogsx2catsdogsx2catsdogsx2catsdogsx2catsdogsx2catsdogsx2catsdogsx2catsdogsx2");
		conn.execute(sql);	
		conn.close();
		conn.close_connection();
	}
	@Test
	public void stringInteger() throws IOException, SQLException, KeyManagementException, NoSuchAlgorithmException, ScriptException, ConnException{
		Connector conn = new Connector("127.0.0.1", 5000, false, false);
		conn.connect("master", "sqream", "sqream", "sqream");
		String sql = String.format("insert into integer_string values (?,?)");
		conn.set_long(1, (long)2008); 
		conn.set_varchar(2, "catsdogsx2catsdogsx2catsdogsx2catsdogsx2catsdogsx2catsdogsx2catsdogsx2catsdogsx2catsdogsx2catsdogsx2");
		conn.execute(sql);
		conn.close();
		conn.close_connection();

	}

	@Test
	public void stringIntegerBatch() throws IOException, SQLException, KeyManagementException, NoSuchAlgorithmException, ScriptException, ConnException{
		Connector conn = new Connector("127.0.0.1", 5000, false, false);
		conn.connect("master", "sqream", "sqream", "sqream");
		String sql = String.format("insert into integer_string values (?,?)");
		conn.execute(sql);
		conn.set_long(1, (long)2008); 
		conn.set_varchar(2, "catsdogsx2catsdogsx2catsdogsx2catsdogsx2catsdogsx2catsdogsx2catsdogsx2catsdogsx2catsdogsx2catsdogsx2");
		conn.next();
		conn.close();
		conn.close_connection();

	}

}

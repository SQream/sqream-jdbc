package com.sqream.jdbc;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;


@RunWith(Suite.class)
@Suite.SuiteClasses({
		//Negative.class,
		JDBC_Positive.class,
		JDBC_Negative.class
		//Batch.class
})

public class JDBCTest {
	public static final String DbName="master";
	public static final String Usr ="sqream";
	public static final String Pswd ="sqream";
	public static final String Host = "127.0.0.1";
	public static final Integer Port = 5000; 
	public static final boolean Ssl = false; 


}

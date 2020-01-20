package com.sqream.jdbc;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import static com.sqream.jdbc.TestEnvironment.*;


@RunWith(Suite.class)
@Suite.SuiteClasses({
		//Negative.class,
		JDBC_Positive.class,
		JDBC_Negative.class
		//Batch.class
})

public class JDBCTest {
	public static final String DbName = DATABASE;
	public static final String Usr = USER;
	public static final String Pswd = PASS;
	public static final String Host = IP;
	public static final Integer Port = PORT;
	public static final boolean Ssl = SSL;


}

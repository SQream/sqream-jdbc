package com.sqream.jdbc;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;

public class Test_Cancel implements Runnable{
 public int x=0;

public static void main(String[] args) throws SQLException, KeyManagementException, NoSuchAlgorithmException, IOException, ClassNotFoundException, InterruptedException{
		
		Class.forName("com.sqream.jdbc.SQDriver");
		
		Test_Cancel t0 = new Test_Cancel();
		t0.x=0;
		Thread thread0 = new Thread(t0);
		thread0.start();
		
		
		
		Test_Cancel t = new Test_Cancel();
		t.x=1;
		Thread thread = new Thread(t);
		thread.start();
		
		Test_Cancel t1 = new Test_Cancel();
		t1.x=1;
		Thread thread1 = new Thread(t1);
		thread1.start();
		
		thread.join();
		thread1.join();
		thread0.join();
		
		System.err.println("QQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQ");
		
		
		
		
	
	}

@Override
public void run() {
	// TODO Auto-generated method stub
	
	TestCase c1 = new TestCase();
	try {
		if(x==0)
			c1.select_sleep();
		else
		    c1.select_case();
	} catch (SQLException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}


}

}

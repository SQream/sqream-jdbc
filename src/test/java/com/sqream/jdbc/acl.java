package com.sqream.jdbc;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.Timestamp;

public class acl {
	
	static void print(Object printable) {
        System.out.println(printable);
    }
	
	
	static String query = "select top 30 message_id ,connectivity_preference ,date_received,enterprise_id ,source_id,template_id,to_email,"
			+ "subject,status,isnull(reason , '') AS reason,submit_time,sender_name,sender_email, var27 from email_combined_history "
			+ "  where date_received >= '2019-03-13 00:00:00.0' and date_received < '2019-03-21 00:00:00.0'order by date_received desc";
	static String query2 = "select top 30 source_id,subject from email_combined_history where date_received >= '2019-03-17 00:00:00.0' and date_received < '2019-03-25 00:00:00.0'order by date_received desc ";
	static String query3 = "select count(1) from otp_dlr ";
	static String query4 = "select top 1 joinid,dlrstatus,requestrecdtime,enterpriseid,messagetext from otp_dlr_history ";
	
	static String query5 = "select top 1 ENTERPRISEID,ENTERPRISENAME,INTFLAG,SUMMARYDATE,SUMMARYFROMTIME,SUMMARYTOTIME,REPORTNAME,SUMMARYLASTUPDATEDTIME from OTP_HDFC_MIS_SUMMARY ";
	
	
	public static void releaseDbConnection(Statement st, ResultSet rs, Connection conn, PreparedStatement preparedStmt)
	{
		try
		{
			if (rs != null)
			{
				rs.close();
			}

			if (st != null)
			{
				st.close();
			}

			if (preparedStmt != null)
			{
				preparedStmt.close();
			}

			if (conn != null)
			{
				conn.close();
			}
		} catch (Exception e)
		{
			e.printStackTrace();
		}
	}
	
	
	
	public static void main(String[] args) {

		try {
			Class.forName("com.sqream.jdbc.SQDriver");
			//Class.forName("sql.SQDriver");

			//Connection con = DriverManager.getConnection("jdbc:Sqream://10.0.6.110:5001/emailstage", "sqream","sqream");
			Connection con = DriverManager.getConnection("jdbc:Sqream://192.168.1.4:5000/master", "sqream","sqream");
			//Connection con = DriverManager.getConnection("jdbc:Sqream://192.168.1.181:3108/otplive;cluster=true", "sqream","sqream");

			PreparedStatement stmt = con.prepareStatement(query5);
			 
			ResultSet rs = stmt.executeQuery();
			//ResultSetMetaData metaData  = rs.getMetaData();
			//int columnCount = metaData.getColumnCount();
			//System.out.println("columnCount::: "+columnCount);
			
			/*
			 * for (int i = 1; i <= columnCount; i++ ) { String name =
			 * metaData.getColumnName(i);
			 * System.out.println("Coulumn Name: "+name+" Type: "+metaData.getColumnType(i)
			 * +" Label : "+metaData.getColumnLabel(i));
			 * 
			 * try { String object = rs.getString(name); System.out.println("object:: "
			 * +object.toString()); // Do stuff with name } catch (Exception e) {
			 * e.printStackTrace(); }
			 * 
			 * }
			 */

			while(rs.next()){
				//RelayReportVo relayStatus=new RelayReportVo();
				
				
				// joinid,dlrstatus,requestrecdtime,enterpriseid,messagetext
				/*
				 * long joinid1 = rs.getLong(1); String dlrstatus1 = rs.getString(2); Timestamp
				 * requestrecdtime1 = rs.getTimestamp(3); String requestrecdtime2 =
				 * rs.getString(3); int enterpriseid1 = rs.getInt(4); String messagetext1 =
				 * rs.getString(5);
				 * 
				 * long joinid = rs.getLong("joinid"); String dlrstatus =
				 * rs.getString("dlrstatus"); Timestamp requestrecdtime =
				 * rs.getTimestamp("requestrecdtime"); String requestrecdtime3 =
				 * rs.getString("requestrecdtime"); int enterpriseid =
				 * rs.getInt("enterpriseid"); String messagetext = rs.getString("messagetext");
				 */
				 
				
				//ENTERPRISEID,ENTERPRISENAME,INTFLAG,SUMMARYDATE,SUMMARYFROMTIME,SUMMARYTOTIME,REPORTNAME,SUMMARYLASTUPDATEDTIME
				  print(rs.getInt("ENTERPRISEID")+"");
				  print(rs.getInt("ENTERPRISEID"));
				  print(rs.getLong("ENTERPRISEID"));
				  print((long)rs.getInt("ENTERPRISEID"));
				  print(rs.getString("ENTERPRISENAME"));
				  print(rs.getInt("INTFLAG")+"");
				  print(rs.getInt("INTFLAG"));
				  print(rs.getLong("INTFLAG"));
				  print(rs.getDate("SUMMARYDATE"));
				  print(rs.getString("SUMMARYDATE"));
				  print(rs.getTimestamp("SUMMARYFROMTIME"));
				  print(rs.getString("SUMMARYFROMTIME"));
				  print(rs.getTimestamp("SUMMARYTOTIME")); 
				  print(rs.getString("SUMMARYTOTIME"));
				  print(rs.getString("REPORTNAME"));
				  print(rs.getTimestamp("SUMMARYLASTUPDATEDTIME"));
				  print(rs.getString("SUMMARYLASTUPDATEDTIME"));
				  
				  //System.out.println(ENTERPRISEID+" :: "+ENTERPRISENAME+" :: "+INTFLAG+" :: "+SUMMARYDATE+" :: "+SUMMARYFROMTIME+" :: "+SUMMARYTOTIME+" :: "+REPORTNAME+" :: "+SUMMARYLASTUPDATEDTIME);
				
				
				//System.out.println("relayStatus:::: "+relayStatus);
			}

			//con.close();
			releaseDbConnection(null,rs,con,stmt);

		} catch (Exception e) {
			//System.out.println(e);
			e.printStackTrace();
		}
		
	}
	

	

}

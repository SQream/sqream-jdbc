package com.sqream.jdbc;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.TimeZone;
import java.util.logging.Logger;

import javax.script.ScriptException;



public class SQDriver implements java.sql.Driver {

	private DriverPropertyInfo[] DPIArray;
	static {
		try {

			DriverManager.registerDriver(new SQDriver());

		} catch (SQLException e) {
			// TODO Auto-generated catch block

			e.printStackTrace();
		}
		 //need to comment UTC line for BG-1729 ,but then we need to handle daylight (same with SqrmTypes and StatementHandle )
		TimeZone.setDefault(TimeZone.getTimeZone("UTC"));

	}

	@Override
	public boolean acceptsURL(String url) throws SQLException {
		// TODO Auto-generated method stub

		uriEx UEX = new uriEx(url); // parse the url to object.
		if (!UEX.provider.toLowerCase().equals("sqream")) {

			return false; // cause it is an other provider, not us..
		}
		return true;
	}

	// Extended uri data
	class uriEx {
		public URI uri;
		public String provider; // hopefully "sqream"
		public String host;
		public int port;
		public String dbName;
		public String user;
		public String pswd;
		public String debug;
		public String logger;
		public String showFullStackTrace;
		public String cluster;
		public String ssl;

		uriEx(String url) throws SQLException {
			try {
				// System.err.println("URL = "+url.substring(5));
				cluster = "false";
				ssl = "false";
				uri = new URI(url.substring(5)); // cause first 5 chars are not
													// relevant
				if (uri == null) {

					throw new SQLException("Connect string general error : " + url
							+ "\nformat Example: 'jdbc:Sqream://<host>:<port>/<dbname>;user=sa;password=sa'");
				}
				if (uri.getPath() == null) {

					throw new SQLException("Connect string general error : " + url
							+ "\nnew format Example: 'jdbc:Sqream://<host>:<port>/<dbname>;user=sa;password=sa'");
				}

				String[] UrlElements = uri.getPath().split(";");
				dbName = UrlElements[0].substring(1);

				String entryType = "";
				String entryValue = "";
				for (String element : UrlElements) {
					if (element.indexOf("=") > 0) {
						// System.out.println("element = "+element);
						String[] entry = element.split("=");
						if (entry.length < 2) {

							throw new SQLException("Connect string error , bad entry element : " + element);
						}

						entryType = entry[0];
						entryValue = entry[1];
						if (entryType.toLowerCase().equals("user"))
							user = entryValue;
						else if (entryType.toLowerCase().equals("password"))
							pswd = entryValue;
						else if (entryType.toLowerCase().equals("logger"))
							logger = entryValue;
						else if (entryType.toLowerCase().equals("debug"))
							debug = entryValue;
						else if (entryType.toLowerCase().equals("showfullstacktrace"))
							showFullStackTrace = entryValue;
						else if (entryType.toLowerCase().equals("cluster"))
							cluster = entryValue;
						else if (entryType.toLowerCase().equals("ssl"))
							ssl = entryValue;
					}
				}
				port = uri.getPort();
				host = uri.getHost();
				provider = uri.getScheme();

			} catch (URISyntaxException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}

	@Override
	public java.sql.Connection connect(String url, Properties info) throws SQLException {

		String prfx = "jdbc:Sqream";

		if (!url.trim().substring(0, prfx.length()).equals(prfx))
			return null; // assaf: don't try to this url, it was not ment for
							// sqream. (propegate it on..)

		if (info == null)
			throw new SQLException("Properties info is null");

		uriEx UEX = new uriEx(url.trim()); // parse the url to object.
		if (!UEX.provider.toLowerCase().equals("sqream")) {

			return null; // cause it is an other provider, not us..
		}

		if (UEX.user == null && info.getProperty("user") == null || UEX.pswd == null && info.getProperty("password") == null) {

			throw new SQLException("please apply user and password"); // in
																		// sqream
																		// it's
																		// a
																		// must..
		}

		if (UEX.user != null) // override the properties object according to
								// Razi.
		{
			info.put("user", UEX.user);
			info.put("password", UEX.pswd);
		}
		// now cast it to JDBC Properties object (cause thats what the
		// DriverManager gives us as parameter):
		if (UEX.dbName == null || UEX.dbName.equals(""))
			throw new SQLException("connection string : missing database name error");
		if (UEX.host == null)
			throw new SQLException("connection string : missing host ip error");
		if (UEX.port == -1)
			throw new SQLException("connection string : missing port error");

		info.put("dbname", UEX.dbName);
		info.put("port", String.valueOf(UEX.port));
		info.put("host", UEX.host);
		info.put("cluster", UEX.cluster);
		info.put("ssl", UEX.ssl);

		Boolean logConfigEnabled = UEX.logger != null ? Boolean.valueOf(UEX.logger) : false;
		Common.setLogEnabled(logConfigEnabled);

		if (UEX.showFullStackTrace != null)
			info.put("showFullStackTrace", UEX.showFullStackTrace);

		SQConnection SQC = null;
		try {
			SQC = new SQConnection(info);
			//String[] lables = { "url", "info" };
			//String[] values = { url, info.toString() };

		} catch (NumberFormatException | IOException | ScriptException e) {
			e.printStackTrace();
			throw new SQLException(e);
		}
		
		return SQC;
	}

	@Override
	public int getMajorVersion() {
		// TODO Auto-generated method stub

		return 4;
	}

	@Override
	public int getMinorVersion() {
		// TODO Auto-generated method stub

		return 0;
	}

	@Override
	public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {

		// TODO Auto-generated method stub
		DPIArray = new DriverPropertyInfo[0];

		return DPIArray;
	}

	@Override
	public boolean jdbcCompliant() {
		// TODO Auto-generated method stub

		return true;
	}

	@Override
	public Logger getParentLogger() throws SQLFeatureNotSupportedException {
		// TODO Auto-generated method stub
		throw new SQLFeatureNotSupportedException();
	}

}

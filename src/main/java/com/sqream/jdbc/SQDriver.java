package com.sqream.jdbc;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.TimeZone;
import java.util.logging.Logger;

import javax.script.ScriptException;

import com.sqream.jdbc.Connector.ConnException;

// Logging
import java.util.Arrays;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.Files;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;


public class SQDriver implements java.sql.Driver {
	
	boolean logging = true;
	Path SQDriver_log = Paths.get("./SQDriver.txt");
	boolean log(String line) throws SQLException {
		if (!logging)
			return true;
		
		try {
			Files.write(SQDriver_log, Arrays.asList(new String[] {line}), UTF_8, CREATE, APPEND);
		} catch (IOException e) {
			e.printStackTrace();
			throw new SQLException ("Error writing to SQDriver log");
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
		public String service;
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
						else if (entryType.toLowerCase().equals("service"))
							service = entryValue;
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
	
	private DriverPropertyInfo[] DPIArray;
	static {
		try {
			DriverManager.registerDriver(new SQDriver());
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	@Override
	public boolean acceptsURL(String url) throws SQLException {
		
		log("inside acceptsURL in SQDriver");
		uriEx UEX = new uriEx(url); // parse the url to object.
		if (!UEX.provider.toLowerCase().equals("sqream")) {

			return false; // cause it is an other provider, not us..
		}
		return true;
	}
	
	@Override
	public java.sql.Connection connect(String url, Properties info) throws SQLException {
		
		log("inside connect in SQDriver");

		String prfx = "jdbc:Sqream";

		if (!url.trim().substring(0, prfx.length()).equals(prfx))
			
			throw new SQLException("Wrong prefix for connection string. Should be jdbc:Sqream but got: " + url.trim().substring(0, prfx.length())); // assaf: don't try to this url, it was not ment for
							// sqream. (propegate it on..)

		if (info == null)
			throw new SQLException("Properties info is null");

		uriEx UEX = new uriEx(url.trim()); // parse the url to object.
		if (!UEX.provider.toLowerCase().equals("sqream")) {

			throw new SQLException("Bad provider in connection string. Should be sqream but got: " + UEX.provider.toLowerCase()); 
		}

		if (UEX.user == null && info.getProperty("user") == null || UEX.pswd == null && info.getProperty("password") == null) {

			throw new SQLException("please apply user and password"); 
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
		
		if(UEX.service != null)
			info.put("service", UEX.service);

		Boolean logConfigEnabled = UEX.logger != null ? Boolean.valueOf(UEX.logger) : false;

		if (UEX.showFullStackTrace != null)
			info.put("showFullStackTrace", UEX.showFullStackTrace);
		//System.out.println ("connection info: " + info);
		SQConnection SQC = null;
		try {
			SQC = new SQConnection(info);
			//String[] lables = { "url", "info" };
			//String[] values = { url, info.toString() };

		} catch (NumberFormatException | IOException | ScriptException| NoSuchAlgorithmException | KeyManagementException | ConnException  e) {
			e.printStackTrace();
			throw new SQLException(e);
		}
		
		return SQC;
	}

	@Override
	public int getMajorVersion() {
		try {
			log("inside getMajorVersion in SQDriver");
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return 4;
	}

	@Override
	public int getMinorVersion() {
		try {
			log("inside getMinorVersion in SQDriver");
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return 0;
	}

	@Override
	public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
		log("inside getPropertyInfo in SQDriver");

		DPIArray = new DriverPropertyInfo[0];

		return DPIArray;
	}

	@Override
	public boolean jdbcCompliant() {
		try {
			log("inside jdbcCompliant in SQDriver");
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return true;
	}

	@Override
	public Logger getParentLogger() throws SQLFeatureNotSupportedException {
		try {
			log("inside getParentLogger in SQDriver");
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		throw new SQLFeatureNotSupportedException();
	}

}

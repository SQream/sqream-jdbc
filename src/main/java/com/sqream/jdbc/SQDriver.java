package com.sqream.jdbc;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.sql.*;
import java.text.MessageFormat;
import java.util.Properties;
import java.util.logging.*;
import java.lang.reflect.Field;
import javax.script.ScriptException;

import com.sqream.jdbc.connector.ConnectorFactory;
import com.sqream.jdbc.connector.ConnException;
import com.sqream.jdbc.logging.LoggingService;
import com.sqream.jdbc.urlParser.URLParser;
import com.sqream.jdbc.urlParser.UrlDto;

import java.nio.charset.Charset;

public class SQDriver implements java.sql.Driver {
	private static final Logger LOGGER = Logger.getLogger(SQDriver.class.getName());

	private static final URLParser urlParser = new URLParser();
	private static final LoggingService loggingService = new LoggingService();

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
		LOGGER.log(Level.FINE, MessageFormat.format("acceptsURL: url=[{0}]", url));

		log("inside acceptsURL in SQDriver");
		UrlDto urlDto = urlParser.parse(url);
		if (urlDto.getProvider() == null || !"sqream".equals(urlDto.getProvider().toLowerCase())) {
			return false; // cause it is an other provider, not us..
		}
		return true;
	}

	@Override
	public Connection connect(String url, Properties info) throws SQLException {
		LOGGER.log(Level.FINE, MessageFormat.format("Connect with params: url=[{0}], info=[{1}]", url, info));

		try {
			System.setProperty("file.encoding","UTF-8");
			Field charset = Charset.class.getDeclaredField("defaultCharset");
			charset.setAccessible(true);
			charset.set(null,null);
		} catch (NoSuchFieldException | SecurityException | IllegalArgumentException | IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		String prfx = "jdbc:Sqream";

		if (!url.trim().substring(0, prfx.length()).equals(prfx))
			
			throw new SQLException("Wrong prefix for connection string. Should be jdbc:Sqream but got: " + url.trim().substring(0, prfx.length())); // assaf: don't try to this url, it was not ment for
							// sqream. (propegate it on..)

		if (info == null)
			throw new SQLException("Properties info is null");

		UrlDto urlDto = urlParser.parse(url);

		loggingService.levelFromUrl(urlDto.getLoggerLevel());
		loggingService.logFilePath(urlDto.getLogFilePath());

		if (!urlDto.getProvider().toLowerCase().equals("sqream")) {
			throw new SQLException("Bad provider in connection string. Should be sqream but got: " + urlDto.getProvider().toLowerCase());
		}

		if (urlDto.getUser() == null && info.getProperty("user") == null || urlDto.getPswd() == null && info.getProperty("password") == null) {
			throw new SQLException("please apply user and password");
		}

		if (urlDto.getUser() != null) {
			info.put("user", urlDto.getUser());
			info.put("password", urlDto.getPswd());
		}
		// now cast it to JDBC Properties object (cause thats what the
		// DriverManager gives us as parameter):
		if (urlDto.getDbName() == null || urlDto.getDbName().equals(""))
			throw new SQLException("connection string : missing database name error");
		if (urlDto.getHost() == null)
			throw new SQLException("connection string : missing host ip error");
		if (urlDto.getPort() == -1)
			throw new SQLException("connection string : missing port error");

		info.put("dbname", urlDto.getDbName());
		info.put("port", String.valueOf(urlDto.getPort()));
		info.put("host", urlDto.getHost());
		info.put("cluster", urlDto.getCluster() != null	? urlDto.getCluster() : "false");
		info.put("ssl", urlDto.getSsl() != null ? urlDto.getSsl() : "false");

		if(urlDto.getService() != null) {
			info.put("service", urlDto.getService());
		}

		Connection SQC;
		try {
			SQC = new SQConnection(info, ConnectorFactory.getFactory());
		} catch (NumberFormatException | ScriptException | IOException | NoSuchAlgorithmException |
		KeyManagementException | ConnException e) {
			e.printStackTrace();
			throw new SQLException(e);
		}

		return SQC;
	}

	@Override
	public int getMajorVersion() {
		log("inside getMajorVersion in SQDriver");
		return 4;
	}

	@Override
	public int getMinorVersion() {
		log("inside getMinorVersion in SQDriver");
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
		log("inside jdbcCompliant in SQDriver");
		return true;
	}

	@Override
	public Logger getParentLogger() {
		LOGGER.log(Level.FINE, MessageFormat.format(
				"Return parent logger [{0}]", LoggingService.getParentLogger()));
		return LoggingService.getParentLogger();
	}

	private void log(String line) {
		LOGGER.log(Level.FINE, line);
	}

}

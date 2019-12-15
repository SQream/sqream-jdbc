package com.sqream.jdbc;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.sql.*;
import java.util.Arrays;
import java.util.Properties;
import java.util.StringJoiner;
import java.util.logging.*;
import java.lang.reflect.Field;
import javax.script.ScriptException;

import com.sqream.jdbc.connector.ConnectorFactory;
import com.sqream.jdbc.connector.ConnException;
import com.sqream.jdbc.enums.LoggerLevel;

import java.nio.charset.Charset;

public class SQDriver implements java.sql.Driver {
	private static final Logger PARENT_LOGGER = Logger.getLogger("com.sqream.jdbc");
	private static final Logger LOGGER = Logger.getLogger(SQDriver.class.getName());
	private static final String PROP_KEY_LOGGER_LEVEL = "loggerLevel";

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
		UriEx uex = new UriEx(url); // parse the url to object.
		if (uex.getProvider() == null || !"sqream".equals(uex.getProvider().toLowerCase())) {
			return false; // cause it is an other provider, not us..
		}
		return true;
	}

	@Override
	public Connection connect(String url, Properties info) throws SQLException {
		setPropsLoggerLevel();
		setUrlLoggerLevel(url);

		log("inside connect in SQDriver");
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

		UriEx UEX = new UriEx(url.trim()); // parse the url to object.
		if (!UEX.getProvider().toLowerCase().equals("sqream")) {

			throw new SQLException("Bad provider in connection string. Should be sqream but got: " + UEX.getProvider().toLowerCase());
		}

		if (UEX.getUser() == null && info.getProperty("user") == null || UEX.getPswd() == null && info.getProperty("password") == null) {

			throw new SQLException("please apply user and password");
		}

		if (UEX.getUser() != null)

		{
			info.put("user", UEX.getUser());
			info.put("password", UEX.getPswd());
		}
		// now cast it to JDBC Properties object (cause thats what the
		// DriverManager gives us as parameter):
		if (UEX.getDbName() == null || UEX.getDbName().equals(""))
			throw new SQLException("connection string : missing database name error");
		if (UEX.getHost() == null)
			throw new SQLException("connection string : missing host ip error");
		if (UEX.getPort() == -1)
			throw new SQLException("connection string : missing port error");

		info.put("dbname", UEX.getDbName());
		info.put("port", String.valueOf(UEX.getPort()));
		info.put("host", UEX.getHost());
		info.put("cluster", UEX.getCluster());
		info.put("ssl", UEX.getSsl());

		if(UEX.getService() != null)
			info.put("service", UEX.getService());

		if (UEX.getShowFullStackTrace() != null) {
			info.put("showFullStackTrace", UEX.getShowFullStackTrace());
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
		log("inside getParentLogger in SQDriver");
		return PARENT_LOGGER;
	}

	private void log(String line) {
		LOGGER.log(Level.FINE, line);
	}

	private void setUrlLoggerLevel(String url) {
		ConsoleHandler consoleHandler = new ConsoleHandler();
		consoleHandler.setLevel(Level.ALL);
		PARENT_LOGGER.addHandler(consoleHandler);
		PARENT_LOGGER.setLevel(Level.OFF);

		int index = url.indexOf("?");
		if (index != -1) {
			String[] props = url.substring(index + 1).split("$");
			Arrays.stream(props)
					.filter(prop -> prop.split("=")[0].equals(PROP_KEY_LOGGER_LEVEL))
					.findFirst()
					.map(prop -> prop.split("=")[1])
					.ifPresent(this::setLevel);
		}
	}

	private void setPropsLoggerLevel() {
		String LOGGING_LEVEL = System.getProperty(PROP_KEY_LOGGER_LEVEL);
		if (LOGGING_LEVEL != null) {
			setLevel(LOGGING_LEVEL);
		}
	}

	private void setLevel(String loggerLevel) {
		switch (LoggerLevel.valueOf(loggerLevel.toUpperCase())) {
			case OFF:
				PARENT_LOGGER.setLevel(Level.OFF);
				break;
			case DEBUG:
				PARENT_LOGGER.setLevel(Level.FINE);
				break;
			case TRACE:
				PARENT_LOGGER.setLevel(Level.FINEST);
				break;
			default:
				StringJoiner supportedLevels = new StringJoiner(", ");
				Arrays.stream(LoggerLevel.values()).forEach(value -> supportedLevels.add(value.getValue()));
				throw new IllegalArgumentException(String.format(
						"Unsupported logging level: %s. Driver supports: %s", loggerLevel, supportedLevels));
		}
	}
}

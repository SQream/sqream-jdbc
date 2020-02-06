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

		Properties props = urlParser.parse(url);
		return "sqream".equalsIgnoreCase(props.getProperty("provider"));
	}

	@Override
	public Connection connect(String url, Properties driverProps) throws SQLException {
		LOGGER.log(Level.FINE, MessageFormat.format("Connect with params: url=[{0}], info=[{1}]", url, driverProps));

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

		if (!url.trim().substring(0, prfx.length()).equals(prfx)) {
			throw new SQLException("Wrong prefix for connection string. Should be jdbc:Sqream but got: " +
					url.trim().substring(0, prfx.length()));
		}

		if (driverProps == null) {
			throw new SQLException("Properties info is null");
		}

		Properties urlProps = urlParser.parse(url);
		Properties defaultProps = createDefaultProps();
		Properties props = PropsParser.merge(urlProps, driverProps, defaultProps);

		if (!"sqream".equalsIgnoreCase(props.getProperty("provider"))) {
			throw new SQLException("Bad provider in connection string. Should be sqream but got: "
					+ props.getProperty("provider"));
		}

		loggingService.levelFromUrl(props.getProperty("loggerLevel"));
		loggingService.logFilePath(props.getProperty("logFile"));

		try {
			return new SQConnection(props, ConnectorFactory.getFactory());
		} catch (NumberFormatException | ScriptException | IOException | NoSuchAlgorithmException |
		KeyManagementException | ConnException e) {
			throw new SQLException(e);
		}
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

	private Properties createDefaultProps() {
		Properties result = new Properties();
		result.put("cluster", "false");
		result.put("ssl", "false");
		result.put("service", "sqream");
		result.put("schema", "public");
		result.put("SkipPicker", "false");// Related to bug #541 - skip the picker if we are in cancel state
		return result;
	}
}

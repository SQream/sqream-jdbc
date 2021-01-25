package com.sqream.jdbc;

import java.sql.*;
import java.text.MessageFormat;
import java.util.Properties;
import java.util.logging.*;

import com.sqream.jdbc.catalogQueryBuilder.CatalogQueryBuilderFactory;
import com.sqream.jdbc.connector.ConnException;
import com.sqream.jdbc.connector.Connector;
import com.sqream.jdbc.connector.ConnectorFactory;
import com.sqream.jdbc.enums.SQSQLState;
import com.sqream.jdbc.logging.LoggingService;
import com.sqream.jdbc.propsParser.CaselessProperties;
import com.sqream.jdbc.propsParser.PropsParser;
import com.sqream.jdbc.propsParser.URLParser;
import com.sqream.jdbc.utils.Utils;

import static com.sqream.jdbc.enums.DriverProperties.*;

public class SQDriver implements java.sql.Driver {
	private static final Logger LOGGER = Logger.getLogger(SQDriver.class.getName());

	private static final String PREFIX = "jdbc:Sqream";
	private static final int MAJOR_VERSION = 4;
	private static final int MINOR_VERSION = 0;

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
		Properties props = new URLParser().parse(url);
		return "sqream".equalsIgnoreCase(props.getProperty("provider"));
	}

	@Override
	public Connection connect(String url, Properties driverProps) throws SQLException {

		String urlPrefix = url.trim().substring(0, PREFIX.length());
		if (!urlPrefix.equals(PREFIX)) {
			return null;
		}

		if (driverProps == null) {
			throw new SQLException("Properties info is null");
		}

		CaselessProperties props = PropsParser.parse(url, driverProps, createDefaultProps());

		if (!validProvider(props)) {
			return null;
		}

		loggingService.set(
				props.getProperty(LOGGER_LEVEL.toString()),
				props.getProperty(LOG_FILE_PATH.toString()));

		LOGGER.log(Level.FINE, MessageFormat.format(
				"Connect with params: url=[{0}], properties=[{1}]", url, driverProps));

		LOGGER.log(Level.FINE, Utils.getMemoryInfo());
		try {
			ConnectionParams connParams = ConnectionParams.builder()
					.cluster(props.getProperty("cluster"))
					.ipAddress(props.getProperty("host"))
					.port(props.getProperty("port"))
					.dbName(props.getProperty("dbName"))
					.service(props.getProperty("service"))
					.schema(props.getProperty("schema"))
					.user(props.getProperty("user"))
					.password(props.getProperty("password"))
					.useSsl(props.getProperty("ssl"))
					.fetchSize(props.getProperty("fetchSize"))
					.insertBuffer(props.getProperty("insertBuffer"))
					.build();

			ConnectorFactory connFactory = getConnectorFactory();
			CatalogQueryBuilderFactory catalogQueryBuilderFactory = getCatalogQueryBuilderFactory();

			return new SQConnection(connParams, connFactory, catalogQueryBuilderFactory);
		} catch (ConnException e) {
			throw addSQLState(e);
		} catch (Exception e) {
			throw new SQLException(e);
		}
	}

	@Override
	public int getMajorVersion() {
		LOGGER.log(Level.FINE, MessageFormat.format("return major version [{0}]", MAJOR_VERSION));
		return MAJOR_VERSION;
	}

	@Override
	public int getMinorVersion() {
		LOGGER.log(Level.FINE, MessageFormat.format("return minor version [{0}]", MINOR_VERSION));
		return MINOR_VERSION;
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
		result.setProperty(CLUSTER.toString(), "false");
		result.setProperty(SSL.toString(), "false");
		result.setProperty(SERVICE.toString(), "sqream");
		result.setProperty(SCHEMA.toString(), "public");
		return result;
	}

	private boolean validProvider(Properties props) {
		return props != null && "sqream".equalsIgnoreCase(props.getProperty("provider"));
	}

	private SQLException addSQLState(ConnException e) {
		if (e != null && e.getMessage() != null) {
			if (e.getMessage().contains("Login failure")) {
				return new SQLException(e.getMessage(), SQSQLState.INVALID_AUTHORIZATION_SPECIFICATION.getState(), e);
			} else if (e.getMessage().contains("Database") && e.getMessage().contains("does not exist")) {
				return new SQLException(e.getMessage(), SQSQLState.INVALID_CATALOG_NAME.getState(), e);
			}
		}
		return new SQLException(e);
	}

	protected ConnectorFactory getConnectorFactory() {
		return new ConnectorFactory();
	}

	protected CatalogQueryBuilderFactory getCatalogQueryBuilderFactory() {
		return new CatalogQueryBuilderFactory();
	}
}

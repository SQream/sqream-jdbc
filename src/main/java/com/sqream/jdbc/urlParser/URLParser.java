package com.sqream.jdbc.urlParser;

import java.net.URI;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.logging.Logger;

public class URLParser {
    private static final Logger LOGGER = Logger.getLogger(URLParser.class.getName());

    private static final String WRONG_FORMAT_EXCEPTION_TEMPLATE = "Connect string general error : [{0}]" +
                    "\nnew format Example: 'jdbc:Sqream://<host>:<port>/<dbname>;user=sa;password=testPassword'";
    private static final Map<String, Consumer<String>> keyMap = new HashMap<>();
    private static UrlDto result;

    static {
        keyMap.put("password", URLParser::setPassword);
        keyMap.put("user", URLParser::setUser);
        keyMap.put("cluster", URLParser::setCluster);
        keyMap.put("ssl", URLParser::setSsl);
        keyMap.put("service", URLParser::setService);
        keyMap.put("loggerLevel", URLParser::setLoggerLevel);
        keyMap.put("logFile", URLParser::setLogFilePath);
    }

    synchronized public UrlDto parse(String url) throws SQLException {
        result = new UrlDto();

        URI uri = parseURI(url);
        result.setProvider(parseProvider(uri));
        result.setHost(parseHost(uri));
        result.setPort(parsePort(uri));

        String[] urlElements = uri.getPath().split(";");
        result.setDbName(urlElements[0].substring(1));
        String curElement;
        for (int i = 1; i < urlElements.length; i++) {
            curElement = urlElements[i];
            if (curElement.indexOf("=") > 0) {
                String[] entry = curElement.split("=");
                if (entry.length != 2) {
                    throw new SQLException(MessageFormat.format(
                            "Connect string error, bad entry element: [{0}]", curElement));
                }
                String entryKey = entry[0];
                String entryValue = entry[1];
                Consumer<String> valueSetter = keyMap.get(entryKey);
                if (valueSetter != null) {
                    valueSetter.accept(entryValue);
                } else {
                    LOGGER.info(MessageFormat.format("Ignore unsupported key in URI [{0}]", entryKey));
                }
            }
        }
        return result;
    }

    private static void setUser(String user) {
        result.setUser(user);
    }

    private static void setPassword(String str) {
        result.setPswd(str);
    }

    private static void setCluster(String cluster) {
        result.setCluster(cluster);
    }

    private static void setSsl(String ssl) {
        result.setSsl(ssl);
    }

    private static void setService(String service) {
        result.setService(service);
    }

    private static void setLoggerLevel(String loggerLevel) {
        result.setLoggerLevel(loggerLevel);
    }

    private static void setLogFilePath(String filePath) {
        result.setLogFilePath(filePath);
    }

    private String parseProvider(URI uri) {
        return uri.getScheme();
    }

    private String parseHost(URI uri) {
        return uri.getHost();
    }

    private int parsePort(URI uri) {
        return uri.getPort();
    }

    private URI parseURI(String url) throws SQLException {
        if (url == null || url.length() < 6) {
            throw new SQLException(MessageFormat.format(WRONG_FORMAT_EXCEPTION_TEMPLATE, url));
        }
        try {
            URI result = new URI(url.substring(5)); // cause first 5 chars are not relevant
            if (result.getPath() == null) {
                throw new SQLException(MessageFormat.format(WRONG_FORMAT_EXCEPTION_TEMPLATE, url));
            }
            return result;
        } catch (URISyntaxException e) {
            throw new SQLException(e.getMessage(), e);
        }
    }
}

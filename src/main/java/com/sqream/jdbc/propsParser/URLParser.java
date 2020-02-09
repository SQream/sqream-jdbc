package com.sqream.jdbc.propsParser;

import java.net.URI;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class URLParser {
    private static final Logger LOGGER = Logger.getLogger(URLParser.class.getName());

    private static final String WRONG_FORMAT_EXCEPTION_TEMPLATE = "Connect string general error : [{0}]" +
                    "\nnew format Example: 'jdbc:Sqream://<host>:<port>/<dbname>;user=sa;password=testPassword'";

    Properties parse(String url) throws SQLException {
        Properties result = new Properties();

        URI uri = parseURI(url);
        result.put("provider", uri.getScheme());
        result.put("host", uri.getHost());
        result.put("port", String.valueOf(uri.getPort()));

        String[] urlElements = uri.getPath().split(";");
        parseUrlElements(urlElements, result);

        LOGGER.log(Level.FINE, MessageFormat.format("Parsed params: [{0}]", convertMapToString(result)));
        return result;
    }

    private void parseUrlElements(String[] elements, Properties target) throws SQLException {
        //first element should be database name
        target.put("dbName", elements[0].substring(1));
        String curElement;
        for (int i = 1; i < elements.length; i++) {
            curElement = elements[i];
            if (curElement.indexOf("=") > 0) {
                String[] entry = curElement.split("=");
                if (entry.length != 2) {
                    throw new SQLException(MessageFormat.format(
                            "Connect string error, bad entry element: [{0}]", curElement));
                }
                String entryKey = entry[0];
                String entryValue = entry[1];
                target.put(entryKey, entryValue);
                LOGGER.log(Level.FINE, MessageFormat.format(
                        "parsed key: [{0}], value: [{1}]", entryKey, entryValue));
            }
        }
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

    private String convertMapToString(Properties props) {
        return props.keySet().stream()
                .map(key -> key + "=" + props.get(key))
                .collect(Collectors.joining(", ", "{", "}"));
    }
}

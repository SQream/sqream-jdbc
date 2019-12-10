package com.sqream.jdbc;

import java.net.URI;
import java.net.URISyntaxException;
import java.sql.SQLException;

// Extended uri data
public class UriEx {
    private URI uri;
    private String provider; // hopefully "sqream"
    private String host;
    private int port;
    private String dbName;
    private String user;
    private String pswd;
    private String debug;
    private String logger;
    private String showFullStackTrace;
    private String cluster = "false";;
    private String service;
    private String ssl = "false";

    UriEx(String url) throws SQLException {
        try {
            this.uri = parseURI(url);
            this.port = uri.getPort();
            this.host = uri.getHost();
            this.provider = uri.getScheme();
            String[] UrlElements = uri.getPath().split(";");
            this.dbName = UrlElements[0].substring(1);

            String entryType = "";
            String entryValue = "";
            for (String element : UrlElements) {
                if (element.indexOf("=") > 0) {
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

        } catch (URISyntaxException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public URI getUri() {
        return uri;
    }

    public String getProvider() {
        return provider;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public String getDbName() {
        return dbName;
    }

    public String getUser() {
        return user;
    }

    public String getPswd() {
        return pswd;
    }

    public String getDebug() {
        return debug;
    }

    public String getLogger() {
        return logger;
    }

    public String getShowFullStackTrace() {
        return showFullStackTrace;
    }

    public String getCluster() {
        return cluster;
    }

    public String getService() {
        return service;
    }

    public String getSsl() {
        return ssl;
    }

    private URI parseURI(String url) throws SQLException, URISyntaxException {
        if (url == null || url.length() < 6) {
            throw new SQLException("Connect string general error : " + url
                    + "\nnew format Example: 'jdbc:Sqream://<host>:<port>/<dbname>;user=sa;password=sa'");
        }
        URI result = new URI(url.substring(5)); // cause first 5 chars are not relevant
        if (result.getPath() == null) {
            throw new SQLException("Connect string general error : " + url
                    + "\nnew format Example: 'jdbc:Sqream://<host>:<port>/<dbname>;user=sa;password=sa'");
        }
        return result;
    }
}

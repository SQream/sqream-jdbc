package com.sqream.jdbc.connector;

import org.apache.maven.artifact.versioning.ComparableVersion;

import java.text.MessageFormat;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ServerVersion implements Comparable<ServerVersion> {
    private static final Logger LOGGER = Logger.getLogger(ServerVersion.class.getName());

    private static final String versionReg = "\\d{4}(\\.\\d+)*";
    private static final String trimReg = "(\\.0)+$";
    private final String version;
    private final String comparableVersion;

    public ServerVersion(String version) {
        this.version = version;
        this.comparableVersion = parse(version);
    }

    public static int compare(String v1, String v2) {
        if (v1 == null || v2 == null) {
            throw new IllegalArgumentException(MessageFormat.format(
                    "Comparable version cannot be null. Compare [{0}] with [{1}]", v1, v2));
        }
        return new ServerVersion(v1).compareTo(new ServerVersion(v2));
    }

    @Override
    public int compareTo(ServerVersion that) {
        if (that == null) {
            throw new IllegalArgumentException("Tried to compare with null");
        }
        return new ComparableVersion(this.comparableVersion)
                .compareTo(new ComparableVersion(that.comparableVersion));
    }

    @Override
    public String toString() {
        return version;
    }

    private String parse(String version) {
        if (version == null) {
            throw new IllegalArgumentException("Parameter 'Version' cannot be null");
        }

        Pattern versionPattern = Pattern.compile(versionReg);
        Matcher versionMatcher = versionPattern.matcher(version);
        String result = "";
        if (versionMatcher.find()) {
            result = versionMatcher.group().replaceAll(trimReg, "");
        } else {
            LOGGER.log(Level.WARNING, MessageFormat.format("Version [{0}] does not match pattern", version));
        }
        return result;
    }
}

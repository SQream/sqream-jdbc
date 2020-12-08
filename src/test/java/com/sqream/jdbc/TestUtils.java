package com.sqream.jdbc;

import com.sqream.jdbc.connector.ConnException;

import java.sql.*;

import static com.sqream.jdbc.TestEnvironment.*;

public class TestUtils {

    /**
     * Checks that all previous statements was closed by running statement "select show_server_status();".
     * Expects only one row in the queue as a result (this statement itself).
     */
    public static boolean isQueueEmpty() {
        boolean result = false;
        try (Connection conn = createConnection();
             Statement stmt = conn.createStatement()) {

            ResultSet rs = stmt.executeQuery("select show_server_status();");
            if (rs.next() && !rs.next()) {
                result = true;
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
        return result;
    }

    public static ConnectionParams createConnectionParams() {
        try {
            return ConnectionParams.builder()
                    .ipAddress(IP)
                    .port(String.valueOf(PORT))
                    .cluster(String.valueOf(CLUSTER))
                    .useSsl(String.valueOf(SSL))
                    .dbName(DATABASE)
                    .user(USER)
                    .password(PASS)
                    .service(SERVICE)
                    .build();
        } catch (ConnException e) {
            throw new RuntimeException(e);
        }
    }
}

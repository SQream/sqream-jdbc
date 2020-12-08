package com.sqream.jdbc.connector.serverAPI.Statement;

import com.sqream.jdbc.ConnectionParams;
import com.sqream.jdbc.connector.serverAPI.SqreamConnection;
import com.sqream.jdbc.connector.serverAPI.SqreamConnectionFactory;
import org.junit.Assert;
import org.junit.Test;

import static com.sqream.jdbc.TestEnvironment.*;
import static com.sqream.jdbc.TestEnvironment.SERVICE;
import static com.sqream.jdbc.TestUtils.isQueueEmpty;

public class SqreamPreparedStatementImplTest {

    @Test
    public void closeTest() throws Exception {
        ConnectionParams connParams = ConnectionParams.builder()
                .ipAddress(IP)
                .port(String.valueOf(PORT))
                .cluster(String.valueOf(CLUSTER))
                .useSsl(String.valueOf(SSL))
                .dbName(DATABASE)
                .user(USER)
                .password(PASS)
                .service(SERVICE)
                .build();

        try (SqreamConnection conn = SqreamConnectionFactory.openConnection(connParams);) {

            SqreamPreparedStatement pstmt = conn.createStatement().prepare("select 1;");

            pstmt.close();

            Assert.assertTrue("Statement was not closed", isQueueEmpty());
        }
    }

}

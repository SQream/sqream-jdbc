package com.sqream.jdbc.connector.serverAPI.Statement;

import com.sqream.jdbc.ConnectionParams;
import com.sqream.jdbc.connector.serverAPI.SqreamConnection;
import com.sqream.jdbc.connector.serverAPI.SqreamConnectionFactory;
import org.junit.Assert;
import org.junit.Test;

import static com.sqream.jdbc.TestEnvironment.*;
import static com.sqream.jdbc.TestEnvironment.SERVICE;
import static junit.framework.TestCase.fail;

public class SqreamCreatedStatementImplTest {

    @Test
    public void prepareTest() throws Exception {
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

        try (SqreamConnection conn = SqreamConnectionFactory.openConnection(connParams)) {
            SqreamCreatedStatement stmt = conn.createStatement();
            stmt.prepare("select 1;");
        }
    }

    @Test(expected = RuntimeException.class)
    public void whenPreparedCalledTwiceThenThrowsExceptionTest() throws Exception {
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

        try (SqreamConnection conn = SqreamConnectionFactory.openConnection(connParams)) {
            SqreamCreatedStatement stmt = conn.createStatement();
            stmt.prepare("select 1;");
            try {
                stmt.prepare("select 2;");
            } catch(RuntimeException e) {
                Assert.assertTrue("Incorrect error message",
                        e.getMessage().contains("Statement has been prepared already"));
                throw e;
            }
            fail("Second call on prepare() should throw exception");
        }
    }
}

package com.sqream.jdbc;

import com.sqream.jdbc.connector.ConnException;
import com.sqream.jdbc.connector.socket.SQSocket;
import com.sqream.jdbc.connector.socket.SQSocketConnector;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.sql.*;
import java.util.Arrays;

import static com.sqream.jdbc.TestEnvironment.*;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;

@RunWith(PowerMockRunner.class)
@PrepareForTest({SQSocketConnector.class, SQSocket.class, SSLContext.class})
public class TODOList {

    //TODO: Should we close SQPreparedStatement#executeQuery() as "Not supported" or implement this logic?
    @Test
    public void executeQueryTest() throws SQLException {
        String CREATE_TABLE_SQL = "create or replace table execute_query_test(col1 int)";
        String INSERT_SQL = "insert into execute_query_test values(?)";
        String SELECT_SQL = "select * from execute_query_test";
        int TEST_VALUE = 42;

        try (Connection conn = createConnection()) {

            try (Statement stmt = conn.createStatement()) {
                stmt.executeUpdate(CREATE_TABLE_SQL);
            }

            try (PreparedStatement pstmt = conn.prepareStatement(INSERT_SQL)) {
                pstmt.setInt(1, TEST_VALUE);
                pstmt.executeQuery();
            }

            try (Statement stmt = conn.createStatement()) {
                ResultSet rs = stmt.executeQuery(SELECT_SQL);
                assertTrue(rs.next());
                assertEquals(TEST_VALUE, rs.getInt(1));
            }
        }
    }

    //TODO: If client set value on the same row few times, value should be overwritten.
    // Now every time when we write into storage, position in byteBuffer moves forward until BufferOverflowException
    @Test
    public void whenSetInPreparedStatementWithoutCallingAddBatchOrExecuteThenValuesAreNotOverwrittenTest() throws SQLException {
        String CREATE_TABLE_SQL = "create or replace table execute_update_test(col1 int)";
        String INSERT_SQL = "insert into execute_update_test values (?)";
        String SELECT_ALL_SQL = "select * from execute_update_test";
        int TEST_VALUE = 42;
        int AMOUNT = 1_000_000_000;

        try (Connection conn = createConnection()) {

            try (Statement stmt = conn.createStatement()) {
                stmt.executeUpdate(CREATE_TABLE_SQL);
            }

            try (PreparedStatement pstmt = conn.prepareStatement(INSERT_SQL)) {
                for (int i = 0; i < AMOUNT; i++) {
                    pstmt.setInt(1, TEST_VALUE);
                }
                pstmt.executeUpdate();
            }

            try (Statement stmt = conn.createStatement()) {
                ResultSet rs = stmt.executeQuery(SELECT_ALL_SQL);

                assertTrue(rs.next());
                assertEquals(TEST_VALUE, rs.getInt(1));
                assertFalse(rs.next());
            }
        }
    }


    //TODO: When we connect to server picker BUT did not provide param 'cluster=true', then read wrong data from socket.
    // Server sent ip address and port to reconnect, but client read first byte as protocol version (actually size of ip address)
    @Test
    public void whenConnectToClusterWithParamClusterFalseTest()
            throws IOException, ConnException, KeyManagementException, NoSuchAlgorithmException {

        SQSocket socketMock = Mockito.mock(SQSocket.class);
        Mockito.when(socketMock.read(any(ByteBuffer.class))).thenAnswer(new Answer<Integer>() {
            @Override
            public Integer answer(InvocationOnMock invocationOnMock) throws Throwable {
                ByteBuffer targetBuffer = invocationOnMock.getArgument(0);
                byte[] ip = IP.getBytes(StandardCharsets.UTF_8);
                targetBuffer.putInt(ip.length);
                targetBuffer.put(Arrays.copyOfRange(ip, 0, targetBuffer.capacity() - targetBuffer.position()));
                return targetBuffer.capacity();
            }
        });

        PowerMockito.mockStatic(SQSocket.class);
        PowerMockito.when(SQSocket.connect(IP, PORT, false)).thenReturn(socketMock);
        PowerMockito.mockStatic(SSLContext.class);
        PowerMockito.when(SSLContext.getDefault()).thenReturn(null);
        SQSocketConnector socketConnector = SQSocketConnector.connect(IP, PORT, false, false);

        socketConnector.parseHeader();
    }


    //TODO: Rewrite commented tests in SQSocketConnectorTest
}

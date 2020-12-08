package com.sqream.jdbc.connector.serverAPI.Statement;

import com.sqream.jdbc.ConnectionParams;
import com.sqream.jdbc.connector.BlockDto;
import com.sqream.jdbc.connector.enums.StatementType;
import com.sqream.jdbc.connector.serverAPI.SqreamConnection;
import com.sqream.jdbc.connector.serverAPI.SqreamConnectionFactory;
import org.junit.Assert;
import org.junit.Test;

import java.sql.*;

import static com.sqream.jdbc.TestEnvironment.createConnection;
import static com.sqream.jdbc.TestUtils.createConnectionParams;
import static com.sqream.jdbc.TestUtils.isQueueEmpty;
import static junit.framework.Assert.assertTrue;
import static junit.framework.TestCase.*;

public class SqreamExecutedStatementImplTest {

    @Test
    public void getSelectTypeTest() throws Exception {
        ConnectionParams connParams = createConnectionParams();
        try (SqreamConnection conn = SqreamConnectionFactory.openConnection(connParams);
             SqreamExecutedStatement stmt = conn.createStatement().prepare("select 1;").execute()) {

            Assert.assertEquals(StatementType.QUERY, stmt.getType());
        }
    }

    @Test
    public void getInsertTypeTest() throws Exception {
        try (Connection conn = createConnection(); Statement stmt = conn.createStatement()) {

            stmt.executeUpdate("create or replace table statement_type_test(col1 int)");
        }

        ConnectionParams connParams = createConnectionParams();
        try (SqreamConnection conn = SqreamConnectionFactory.openConnection(connParams);
             SqreamExecutedStatement stmt =
                     conn.createStatement().prepare("insert into statement_type_test values(?);").execute()) {

            Assert.assertEquals(StatementType.NETWORK_INSERT, stmt.getType());
        }
    }

    @Test
    public void getDMLTypeTest() throws Exception {
        ConnectionParams connParams = createConnectionParams();
        try (SqreamConnection conn = SqreamConnectionFactory.openConnection(connParams);
             SqreamExecutedStatement stmt = conn.createStatement().prepare(
                     "create or replace table statement_type_test(col1 int)").execute()) {

            Assert.assertEquals(StatementType.NON_QUERY, stmt.getType());
        }
    }

    @Test
    public void closeTest() throws Exception {
        ConnectionParams connParams = createConnectionParams();
        try (SqreamConnection conn = SqreamConnectionFactory.openConnection(connParams)) {
            SqreamExecutedStatement stmt = conn.createStatement().prepare("select 1;").execute();

            stmt.close();

            Assert.assertTrue("Statement was not closed", isQueueEmpty());
        }
    }

    @Test
    public void whenCallCloseTwiceThenSecondIgnoredTest() throws Exception {
        ConnectionParams connParams = createConnectionParams();
        try (SqreamConnection conn = SqreamConnectionFactory.openConnection(connParams)) {
            SqreamExecutedStatement stmt = conn.createStatement().prepare("select 1;").execute();

            stmt.close();
            stmt.close();
        }
    }

    @Test
    public void fetchTest() throws Exception {
        ConnectionParams connParams = createConnectionParams();
        try (SqreamConnection conn = SqreamConnectionFactory.openConnection(connParams);
             SqreamExecutedStatement stmt = conn.createStatement().prepare("select 1;").execute()) {

            Assert.assertEquals(StatementType.QUERY, stmt.getType());
            Assert.assertEquals(1, stmt.fetch().getCapacity());
            Assert.assertEquals(0, stmt.fetch().getCapacity());
        }
    }

    @Test(expected = IllegalStateException.class)
    public void whenCallFetchOnNotSelectStatementThenThrowsException() throws Exception {
        ConnectionParams connParams = createConnectionParams();
        try (SqreamConnection conn = SqreamConnectionFactory.openConnection(connParams);
             SqreamExecutedStatement stmt =
                     conn.createStatement().prepare("create or replace table fetch_test(col1 int);").execute()) {

            stmt.fetch();
        } catch (IllegalStateException e) {
            Assert.assertTrue(e.getMessage().contains("is not a select statement"));
            throw e;
        }
        fail("Should throw IllegalStateException exception");
    }

    @Test
    public void putTest() throws Exception {
        int rowAmount = 42;

        // prepare test data
        try (Connection conn = createConnection()) {

            try (Statement stmt = conn.createStatement()) {
                stmt.executeUpdate("create or replace table put_test(col1 int);");
            }

            try (PreparedStatement pstmt = conn.prepareStatement("insert into put_test values(?);")) {
                for (int i = 0; i < 42; i++) {
                    pstmt.setInt(1, i);
                    pstmt.addBatch();
                }
                pstmt.executeBatch();
            }
        }

        // read BlockDto
        BlockDto testBlock = null;
        try (SqreamConnection conn = SqreamConnectionFactory.openConnection(createConnectionParams());
             SqreamExecutedStatement stmt =
                     conn.createStatement().prepare("select * from put_test").execute()) {

            assertEquals(StatementType.QUERY, stmt.getType());
            testBlock = stmt.fetch();
        }
        assertNotNull(testBlock);
        assertEquals(rowAmount, testBlock.getFillSize());

        // put test block
        try (SqreamConnection conn = SqreamConnectionFactory.openConnection(createConnectionParams());
             SqreamExecutedStatement stmt =
                     conn.createStatement().prepare("insert into put_test values(?);").execute()) {

            assertEquals(StatementType.NETWORK_INSERT, stmt.getType());
            stmt.put(testBlock);
        }

        // validate row amount
        try (Connection conn = createConnection();
             Statement stmt = conn.createStatement()) {

            ResultSet rs = stmt.executeQuery("select count(*) from put_test;");
            assertTrue(rs.next());
            assertEquals(rowAmount * 2, rs.getInt(1));
        }
    }

    @Test(expected = IllegalStateException.class)
    public void whenCallPutOnNotInsertStatementThenThrowsException() throws Exception {
        ConnectionParams connParams = createConnectionParams();
        try (SqreamConnection conn = SqreamConnectionFactory.openConnection(connParams);
             SqreamExecutedStatement stmt =
                     conn.createStatement().prepare("select 1;").execute()) {

            stmt.put(null);
        } catch (IllegalStateException e) {
            Assert.assertTrue(e.getMessage().contains("is not an insert statement"));
            throw e;
        }
        fail("Should throw IllegalStateException exception");
    }
}

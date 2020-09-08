package com.sqream.jdbc.connector.messenger;

import com.eclipsesource.json.Json;
import com.eclipsesource.json.JsonObject;
import com.eclipsesource.json.ParseException;
import com.sqream.jdbc.connector.*;
import com.sqream.jdbc.connector.socket.SQSocketConnector;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static com.sqream.jdbc.TestEnvironment.IP;
import static com.sqream.jdbc.TestEnvironment.PORT;
import static com.sqream.jdbc.connector.JsonParser.TEXT_ITEM_SIZE;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.powermock.api.mockito.PowerMockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest({Json.class})
public class MessengerImplTest {

    private static final String EXPECTED_PART_OF_EXCEPTION = "Expected message:";

    private Messenger messenger;
    private SQSocketConnector socket;

    @Before
    public void setUp() {
        this.socket = Mockito.mock(SQSocketConnector.class);
        this.messenger = MessengerImpl.getInstance(socket);
        when(socket.generateHeaderedBuffer(any(Long.class), any(Boolean.class))).thenCallRealMethod();
    }

    @Test
    public void fetchTest() throws IOException, ConnException {
        String expectedResponse = "{\"colSzs\":[1048576,4194304],\"rows\":1048576}";
        String expectedRequest = "{\"fetch\":\"fetch\"}";
        ByteBuffer buffer = socket.generateHeaderedBuffer(expectedRequest.length(), true);
        buffer.put(expectedRequest.getBytes());
        Mockito.when(socket.sendData(buffer, true)).thenReturn(expectedResponse);

        FetchMetadataDto result = messenger.fetch();

        assertNotNull(result);
        assertEquals(1048576, result.getNewRowsFetched());
        assertEquals(2, result.colAmount());
        assertEquals(1048576, result.getSizeByIndex(0));
        assertEquals(4194304, result.getSizeByIndex(1));
    }

    @Test
    public void connectTest() throws IOException, ConnException {
        String connectionRequest = "{\"connectDatabase\":\"testDatabase\", \"username\":\"testUser\", \"password\":\"testPass\", \"service\":\"testService\"}";
        ByteBuffer buffer = socket.generateHeaderedBuffer(connectionRequest.length(), true);
        buffer.put(connectionRequest.getBytes());
        Mockito.when(socket.sendData(buffer, true)).thenReturn("{\"connectionId\":1139,\"databaseConnected\":\"databaseConnected\",\"varcharEncoding\":\"cp874\"}\n");

        ConnectionStateDto state = messenger.connect("testDatabase", "testUser", "testPass", "testService");

        assertNotNull(state);
        assertEquals(1139, state.getConnectionId());
        assertEquals("cp874", state.getVarcharEncoding());
    }

    @Test
    public void reconnectTest() throws IOException, ConnException {
        String reconnectionRequest = "{\"reconnectDatabase\":\"testDatabase\", \"username\":\"testUser\", \"password\":\"testPass\", \"service\":\"testService\", \"connectionId\": 1139, \"listenerId\": 1}";
        ByteBuffer buffer = socket.generateHeaderedBuffer(reconnectionRequest.length(), true);
        buffer.put(reconnectionRequest.getBytes());
        messenger.reconnect("testDatabase", "testUser", "testPass", "testService", 1139, 1);

        Mockito.verify(socket).sendData(buffer, true);
    }

    @Test
    public void closeConnectionTest() throws IOException, ConnException {
        String closeConnectionRequest = "{\"closeConnection\":\"closeConnection\"}";
        ByteBuffer buffer = socket.generateHeaderedBuffer(closeConnectionRequest.length(), true);
        buffer.put(closeConnectionRequest.getBytes());
        Mockito.when(socket.sendData(buffer, true)).thenReturn("{\"connectionClosed\":\"connectionClosed\"}");

        messenger.closeConnection();
    }

    @Test(expected = ConnException.class)
    public void closeConnectionWrongResponseTest() throws IOException, ConnException {
        String closeConnectionRequest = "{\"closeConnection\":\"closeConnection\"}";
        ByteBuffer buffer = socket.generateHeaderedBuffer(closeConnectionRequest.length(), true);
        buffer.put(closeConnectionRequest.getBytes());
        Mockito.when(socket.sendData(buffer, true)).thenReturn("wrong answer from server");

        try {
            messenger.closeConnection();
        } catch (ConnException e) {
            if (e.getMessage().contains(EXPECTED_PART_OF_EXCEPTION)) {
                throw e;
            } else {
                throw new RuntimeException("Wrong message exception", e);
            }
        }
    }

    @Test(expected = ConnException.class)
    public void closeConnectionErrorResponseTest() throws IOException, ConnException {
        String errorResponse = "{\"error\":\"error response\"}";

        String closeConnectionRequest = "{\"closeConnection\":\"closeConnection\"}";
        ByteBuffer buffer = socket.generateHeaderedBuffer(closeConnectionRequest.length(), true);
        buffer.put(closeConnectionRequest.getBytes());
        Mockito.when(socket.sendData(buffer, true)).thenReturn(errorResponse);

        try {
            messenger.closeConnection();
        } catch (ConnException e) {
            if (e.getMessage().equals("error response")) {
                throw e;
            } else {
                throw new RuntimeException("Wrong message exception", e);
            }
        }
    }

    @Test
    public void closeStatementTest() throws IOException, ConnException {
        String closeStatementRequest = "{\"closeStatement\":\"closeStatement\"}";
        ByteBuffer buffer = socket.generateHeaderedBuffer(closeStatementRequest.length(), true);
        buffer.put(closeStatementRequest.getBytes());
        Mockito.when(socket.sendData(buffer, true)).thenReturn("{\"statementClosed\":\"statementClosed\"}");

        messenger.closeStatement();
    }

    @Test(expected = ConnException.class)
    public void closeStatementWrongResponseTest() throws IOException, ConnException {
        String closeStatementRequest = "{\"closeStatement\":\"closeStatement\"}";
        ByteBuffer buffer = socket.generateHeaderedBuffer(closeStatementRequest.length(), true);
        buffer.put(closeStatementRequest.getBytes());
        Mockito.when(socket.sendData(buffer, true)).thenReturn("wrong:response");

        try {
            messenger.closeStatement();
        } catch (ConnException e) {
            if (e.getMessage().contains(EXPECTED_PART_OF_EXCEPTION)) {
                throw e;
            } else {
                throw new RuntimeException("Wrong message exception", e);
            }
        }
    }

    @Test(expected = ConnException.class)
    public void closeStatementErrorResponseTest() throws IOException, ConnException {
        String errorResponse = "{\"error\":\"error response\"}";

        String closeStatementRequest = "{\"closeStatement\":\"closeStatement\"}";
        ByteBuffer buffer = socket.generateHeaderedBuffer(closeStatementRequest.length(), true);
        buffer.put(closeStatementRequest.getBytes());
        Mockito.when(socket.sendData(buffer, true)).thenReturn(errorResponse);

        try {
            messenger.closeStatement();
        } catch (ConnException e) {
            if (e.getMessage().equals("error response")) {
                throw e;
            } else {
                throw new RuntimeException("Wrong message exception", e);
            }
        }
    }

    @Test
    public void putTest() throws IOException, ConnException {
        String putRequest = "{\"put\": 15}";
        ByteBuffer buffer = socket.generateHeaderedBuffer(putRequest.length(), true);
        buffer.put(putRequest.getBytes());
        messenger.put(15);

        Mockito.verify(socket).sendData(buffer, false);
    }

    @Test
    public void isPuttedTest() throws IOException, ConnException {
        Mockito.when(socket.sendData(null, true)).thenReturn("{\"putted\":\"putted\"}");

        messenger.isPutted();
    }

    @Test(expected = ConnException.class)
    public void isPuttedGetWrongResponseTest() throws IOException, ConnException {
        Mockito.when(socket.sendData(null, true)).thenReturn("wrong:response");

        try {
            messenger.isPutted();
        } catch (ConnException e) {
            if (e.getMessage().contains(EXPECTED_PART_OF_EXCEPTION)) {
                throw e;
            } else {
                throw new RuntimeException("Wrong message exception", e);
            }
        }
    }

    @Test
    public void isStatementReconstructedTest() throws IOException, ConnException {
        String isStatementReconstructedRequest = "{\"reconstructStatement\": 1083}";
        ByteBuffer buffer = socket.generateHeaderedBuffer(isStatementReconstructedRequest.length(), true);
        buffer.put(isStatementReconstructedRequest.getBytes());

        Mockito.when(socket.sendData(buffer, true)).thenReturn("{\"statementReconstructed\":\"statementReconstructed\"}");

        messenger.isStatementReconstructed(1083);
    }

    @Test(expected = ConnException.class)
    public void isStatementReconstructedGetWrongResponseTest() throws IOException, ConnException {
        String isStatementReconstructedRequest = "{\"reconstructStatement\": 1083}";
        ByteBuffer buffer = socket.generateHeaderedBuffer(isStatementReconstructedRequest.length(), true);
        buffer.put(isStatementReconstructedRequest.getBytes());

        Mockito.when(socket.sendData(buffer, true)).thenReturn("wrong:response");

        messenger.isStatementReconstructed(1083);
    }

    @Test
    public void openStatementTest() throws IOException, ConnException {
        String getStatementIdRequest = "{\"getStatementId\":\"getStatementId\"}";
        ByteBuffer buffer = socket.generateHeaderedBuffer(getStatementIdRequest.length(), true);
        buffer.put(getStatementIdRequest.getBytes());

        Mockito.when(socket.sendData(buffer, true)).thenReturn("{\"statementId\":1083}");

        assertEquals(1083, messenger.openStatement());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void openStatementGetWrongResponseTest() throws IOException, ConnException {
        String getStatementIdRequest = "{\"getStatementId\":\"getStatementId\"}";
        ByteBuffer buffer = socket.generateHeaderedBuffer(getStatementIdRequest.length(), true);
        buffer.put(getStatementIdRequest.getBytes());

        Mockito.when(socket.sendData(buffer, true)).thenReturn("{\"statementId\":\"notNumber\"}");

        messenger.openStatement();
    }

    @Test
    public void queryTypeInputTest() throws IOException, ConnException {
        String queryTypeInputRequest = "{\"queryTypeIn\":\"queryTypeIn\"}";
        String serverResponse = "{\"queryType\":[{\"isTrueVarChar\":false,\"nullable\":true,\"type\":[\"ftBool\",1,0]},{\"isTrueVarChar\":false,\"nullable\":true,\"type\":[\"ftUByte\",1,0]},{\"isTrueVarChar\":false,\"nullable\":true,\"type\":[\"ftShort\",2,0]},{\"isTrueVarChar\":true,\"nullable\":true,\"type\":[\"ftBlob\",0,0]}]}";
        List<ColumnMetadataDto> expectedList = new ArrayList<>();
        expectedList.add(new ColumnMetadataDto(false, "", true, "ftBool", 1));
        expectedList.add(new ColumnMetadataDto(false, "", true, "ftUByte", 1));
        expectedList.add(new ColumnMetadataDto(false, "", true, "ftShort", 2));
        expectedList.add(new ColumnMetadataDto(true, "", true, "ftBlob", TEXT_ITEM_SIZE));
        ByteBuffer buffer = socket.generateHeaderedBuffer(queryTypeInputRequest.length(), true);
        buffer.put(queryTypeInputRequest.getBytes());
        Mockito.when(socket.sendData(buffer, true)).thenReturn(serverResponse);

        List<ColumnMetadataDto> resultList = messenger.queryTypeInput();

        assertNotNull(resultList);
        ColumnMetadataDto expected;
        ColumnMetadataDto result;
        for (int i = 0; i < expectedList.size(); i++) {
            expected = expectedList.get(i);
            result = resultList.get(i);
            assertEquals(expected.getValueSize(), result.getValueSize());
            assertEquals(expected.isTruVarchar(), result.isTruVarchar());
            assertEquals(expected.isNullable(), result.isNullable());
            assertEquals(expected.getValueType(), result.getValueType());
            assertEquals(expected.getName(), result.getName());
        }
    }

    @Test
    public void queryTypeOutTest() throws IOException, ConnException {
        String queryTypeOutRequest = "{\"queryTypeOut\":\"queryTypeOut\"}";
        String serverResponse = "{\"queryTypeNamed\":[{\"isTrueVarChar\":false,\"name\":\"ints\",\"nullable\":true,\"type\":[\"ftInt\",4,0]}]}";
        List<ColumnMetadataDto> expectedList = new ArrayList<>();
        expectedList.add(new ColumnMetadataDto(false, "ints", true, "ftInt", 4));
        ByteBuffer buffer = socket.generateHeaderedBuffer(queryTypeOutRequest.length(), true);
        buffer.put(queryTypeOutRequest.getBytes());
        Mockito.when(socket.sendData(buffer, true)).thenReturn(serverResponse);

        List<ColumnMetadataDto> resultList = messenger.queryTypeOut();

        assertNotNull(resultList);
        ColumnMetadataDto expected;
        ColumnMetadataDto result;
        for (int i = 0; i < expectedList.size(); i++) {
            expected = expectedList.get(i);
            result = resultList.get(i);
            assertEquals(expected.getValueSize(), result.getValueSize());
            assertEquals(expected.isTruVarchar(), result.isTruVarchar());
            assertEquals(expected.isNullable(), result.isNullable());
            assertEquals(expected.getValueType(), result.getValueType());
            assertEquals(expected.getName(), result.getName());
        }
    }

    @Test
    public void executeTest() throws IOException, ConnException {
        String executeRequest = "{\"execute\":\"execute\"}";
        String response = "{\"executed\":\"executed\"}";
        ByteBuffer buffer = socket.generateHeaderedBuffer(executeRequest.length(), true);
        buffer.put(executeRequest.getBytes());
        Mockito.when(socket.sendData(buffer, true)).thenReturn(response);

        messenger.execute();
    }

    @Test(expected = ConnException.class)
    public void executeGetWrongResponseTest() throws IOException, ConnException {
        String executeRequest = "{\"execute\":\"execute\"}";
        ByteBuffer buffer = socket.generateHeaderedBuffer(executeRequest.length(), true);
        buffer.put(executeRequest.getBytes());
        Mockito.when(socket.sendData(buffer, true)).thenReturn("wrong:response");

        try {
            messenger.execute();
        } catch (ConnException e) {
            if (e.getMessage().contains(EXPECTED_PART_OF_EXCEPTION)) {
                throw e;
            } else {
                throw new RuntimeException("Wrong message exception", e);
            }
        }
    }

    @Test
    public void prepareStatementTest() throws IOException, ConnException {
        String statement = "select * from test_table;";
        int chunkSize = 1_000;
        int listenerId = 1;
        int portSsl = 5001;
        JsonObject request = new JsonObject();
        request.set("prepareStatement", statement);
        request.set("chunkSize", chunkSize);
        JsonObject response = new JsonObject();
        response.set("ip", IP);
        response.set("listener_id", listenerId);
        response.set("port", PORT);
        response.set("port_ssl", portSsl);
        response.set("reconnect", true);
        response.set("statementPrepared", true);
        ByteBuffer buffer = socket.generateHeaderedBuffer(request.toString().length(), true);
        buffer.put(request.toString().getBytes());
        Mockito.when(socket.sendData(buffer, true)).thenReturn(response.toString());

        StatementStateDto result = messenger.prepareStatement(statement, chunkSize);

        assertEquals(result.getIp(), IP);
        assertEquals(result.getListenerId(), listenerId);
        assertEquals(result.getPort(), PORT);
        assertEquals(result.getPortSsl(), portSsl);
        assertTrue(result.isReconnect());
    }

    @Test(expected = ConnException.class)
    public void whenJsonParserThrowsExceptionThenPrepareStatementWrapItTest() throws IOException, ConnException {
        String statement = "select * from test_table;";
        int chunkSize = 1_000;
        JsonObject jsonObjectMock = Mockito.mock(JsonObject.class);
        Mockito.when(jsonObjectMock.add(any(String.class), any(String.class))).thenThrow(ParseException.class);
        PowerMockito.mockStatic(Json.class);
        when(Json.object()).thenReturn(jsonObjectMock);

        messenger.prepareStatement(statement, chunkSize);
    }
}
package com.sqream.jdbc.connector;

import com.eclipsesource.json.JsonArray;
import com.eclipsesource.json.JsonObject;
import org.junit.Test;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import static com.sqream.jdbc.connector.JsonParser.TEXT_ITEM_SIZE;
import static org.junit.Assert.*;

public class JsonParserTest {

    private JsonParser parser = new JsonParser();

    @Test
    public void toConnectionStateTest() throws ConnException {
        int CONNECTION_ID = 123;
        String ENCODING = "cp874";
        String JSON = jsonBuilder()
                .connectionId(CONNECTION_ID)
                .varcharEncoding(ENCODING)
                .build();
        ConnectionStateDto expected = new ConnectionStateDto(CONNECTION_ID, ENCODING);

        ConnectionStateDto result = parser.toConnectionState(JSON);

        assertNotNull(result);
        assertEquals(expected.getConnectionId(), result.getConnectionId());
        assertEquals(expected.getVarcharEncoding(), result.getVarcharEncoding());
    }

    @Test
    public void checkDefaultVarcharEncoding() throws ConnException {
        int CONNECTION_ID = 123;
        String DEFAULT_ENCODING = "ascii";
        String JSON_WITHOUT_ENCODING = jsonBuilder()
                .connectionId(CONNECTION_ID)
                .build();
        ConnectionStateDto expected = new ConnectionStateDto(CONNECTION_ID, DEFAULT_ENCODING);

        ConnectionStateDto result = parser.toConnectionState(JSON_WITHOUT_ENCODING);

        assertNotNull(result);
        assertEquals(expected.getVarcharEncoding(), result.getVarcharEncoding());
    }

    @Test
    public void checkCP874VarcharEncoding() throws ConnException {
        int CONNECTION_ID = 123;
        String ENCODING_CONTAINS_874 = "someEncodingContains874-*&%#$@";
        String EXPECTED_ENCODING = "cp874";
        String JSON = jsonBuilder()
                .connectionId(CONNECTION_ID)
                .varcharEncoding(ENCODING_CONTAINS_874)
                .build();
        ConnectionStateDto expected = new ConnectionStateDto(CONNECTION_ID, EXPECTED_ENCODING);

        ConnectionStateDto result = parser.toConnectionState(JSON);

        assertNotNull(result);
        assertEquals(expected.getVarcharEncoding(), result.getVarcharEncoding());
    }

    @Test
    public void whenQueryTypeInIsEmptyArrayTest() throws ConnException {
        String json = "{\"queryType\":[]}";

        List<ColumnMetadataDto> resultList = parser.toQueryTypeInput(json);

        assertNotNull(resultList);
        assertTrue(resultList.isEmpty());
    }

    @Test
    public void toQueryTypeInputTest() throws ConnException {
        String JSON = "{" +
                "  \"queryType\": [" +
                "    {" +
                "      \"name\": \"someName\"," +
                "      \"isTrueVarChar\": false," +
                "      \"nullable\": true," +
                "      \"type\": [" +
                "        \"ftBool\"," +
                "        1," +
                "        0" +
                "      ]" +
                "    }," +
                "    {" +
                "      \"isTrueVarChar\": true," +
                "      \"nullable\": false," +
                "      \"type\": [" +
                "        \"ftBlob\"," +
                "        0," +
                "        0" +
                "      ]" +
                "    }" +
                "  ]" +
                "}";
        List<ColumnMetadataDto> expectedList = new LinkedList<>();
        expectedList.add(new ColumnMetadataDto(false, "someName", true, "ftBool", 1, 0));
        // name = "" by default
        // if itemSize == 0 then set as TEXT_ITEM_SIZE
        expectedList.add(new ColumnMetadataDto(true, "", false, "ftBlob", TEXT_ITEM_SIZE, 0));

        List<ColumnMetadataDto> resultList = parser.toQueryTypeInput(JSON);

        assertEquals(expectedList.size(), resultList.size());
        for (int i = 0; i < expectedList.size(); i++) {
            assertEquals(expectedList.get(i).getName(), resultList.get(i).getName());
            assertEquals(expectedList.get(i).getValueType(), resultList.get(i).getValueType());
            assertEquals(expectedList.get(i).getValueSize(), resultList.get(i).getValueSize());
            assertEquals(expectedList.get(i).isNullable(), resultList.get(i).isNullable());
            assertEquals(expectedList.get(i).isTruVarchar(), resultList.get(i).isTruVarchar());
        }
    }

    @Test
    public void whenQueryTypeOutIsEmptyArrayTest() throws ConnException {
        String json = "{\"queryTypeNamed\":[]}";

        List<ColumnMetadataDto> resultList = parser.toQueryTypeOut(json);

        assertNotNull(resultList);
        assertTrue(resultList.isEmpty());
    }

    @Test(expected = ConnException.class)
    public void whenJsonHasErrorThenToConnectionStateThrowsExceptionTest() throws ConnException {
        parser.toConnectionState(generateJsonWithError());
    }

    @Test(expected = ConnException.class)
    public void whenJsonHasErrorThenToQueryTypeInputThrowsExceptionTest() throws ConnException {
        parser.toQueryTypeInput(generateJsonWithError());
    }

    @Test(expected = ConnException.class)
    public void whenJsonHasErrorThenToQueryTypeOutThrowsExceptionTest() throws ConnException {
        parser.toQueryTypeOut(generateJsonWithError());
    }

    @Test(expected = ConnException.class)
    public void whenJsonHasErrorThenToFetchMetadataThrowsExceptionTest() throws ConnException {
        parser.toFetchMetadata(generateJsonWithError());
    }

    @Test(expected = ConnException.class)
    public void whenJsonHasErrorThenToStatementIdThrowsExceptionTest() throws ConnException {
        parser.toStatementId(generateJsonWithError());
    }

    @Test(expected = ConnException.class)
    public void whenJsonHasErrorThenToStatementStateThrowsExceptionTest() throws ConnException {
        parser.toStatementState(generateJsonWithError());
    }

    @Test(expected = ConnException.class)
    public void toConnectionStateGetEmptyStateTest() throws ConnException {
        String json = jsonBuilder().build();
        parser.toConnectionState(json);
    }

    @Test
    public void toConnectionStateGetStateWithoutVarcharEncodingTest() throws ConnException {
        String json = jsonBuilder().connectionId(1).build();
        parser.toConnectionState(json);
    }

    @Test(expected = ConnException.class)
    public void toFetchMetadataGetEmptyJsonTest() throws ConnException {
        String json = jsonBuilder().build();
        parser.toFetchMetadata(json);
    }

    @Test(expected = ConnException.class)
    public void toFetchMetadataGetJsonWithoutColSzsTest() throws ConnException {
        String json = jsonBuilder().rows(1).build();
        parser.toFetchMetadata(json);
    }

    @Test(expected = ConnException.class)
    public void toStatementIdGetEmptyJsonTest() throws ConnException {
        String json = jsonBuilder().build();
        parser.toStatementId(json);
    }

    @Test(expected = ConnException.class)
    public void toStatementStateGetEmptyJson() throws ConnException {
        String json = jsonBuilder().build();
        parser.toStatementState(json);
    }

    @Test(expected = ConnException.class)
    public void toStatementStateGetJsonWithoutPortTest() throws ConnException {
        String json = jsonBuilder()
                .listenerId(1)
                .portSsl(8080)
                .reconnect(false)
                .ip("8.8.8.8")
                .build();
        parser.toStatementState(json);
    }

    @Test(expected = ConnException.class)
    public void toStatementStateGetJsonWithoutPortSslTest() throws ConnException {
        String json = jsonBuilder()
                .listenerId(1)
                .port(8080)
                .reconnect(false)
                .ip("8.8.8.8")
                .build();
        parser.toStatementState(json);
    }

    @Test(expected = ConnException.class)
    public void toStatementStateGetJsonWithoutReconnectTest() throws ConnException {
        String json = jsonBuilder()
                .listenerId(1)
                .port(8080)
                .portSsl(8080)
                .ip("8.8.8.8")
                .build();
        parser.toStatementState(json);
    }

    @Test(expected = ConnException.class)
    public void toStatementStateGetJsonWithoutIpTest() throws ConnException {
        String json = jsonBuilder()
                .listenerId(1)
                .port(8080)
                .portSsl(8080)
                .reconnect(false)
                .build();
        parser.toStatementState(json);
    }

    @Test
    public void toStatementStateTest() throws ConnException {
        String json = jsonBuilder()
                .listenerId(1)
                .port(8080)
                .portSsl(8080)
                .reconnect(false)
                .ip("8.8.8.8")
                .build();
        parser.toStatementState(json);
    }

    private String generateJsonWithError() {
        return "{\"error\":\"some  error message\"}";
    }

    private TestJsonBuilder jsonBuilder() {
        return new TestJsonBuilder();
    }

    private static class TestJsonBuilder {
        private Integer connectionId;
        private String varcharEncoding;
        private Integer rows;
        private int[] colSzs;
        private Integer listenerId;
        private Integer port;
        private Integer portSsl;
        private Boolean reconnect;
        private String ip;

        private  TestJsonBuilder() { }

        TestJsonBuilder connectionId(int id) {
            this.connectionId = id;
            return this;
        }

        TestJsonBuilder varcharEncoding(String encoding) {
            this.varcharEncoding = encoding;
            return this;
        }

        TestJsonBuilder rows(int rows) {
            this.rows = rows;
            return this;
        }

        TestJsonBuilder colSzs(int[] colSzs) {
            this.colSzs = colSzs;
            return this;
        }

        TestJsonBuilder listenerId(int id) {
            this.listenerId = id;
            return this;
        }

        TestJsonBuilder port(int port) {
            this.port = port;
            return this;
        }

        TestJsonBuilder portSsl(int portSsl) {
            this.portSsl = portSsl;
            return this;
        }

        TestJsonBuilder reconnect(boolean reconnect) {
            this.reconnect = reconnect;
            return this;
        }

        TestJsonBuilder ip(String ip) {
            this.ip = ip;
            return this;
        }

        String build() {
            JsonObject result = new JsonObject();
            if (connectionId != null) {
                result.set("connectionId", connectionId);
            }
            if (varcharEncoding != null) {
                result.set("varcharEncoding", varcharEncoding);
            }
            if (rows != null) {
                result.set("rows", rows);
            }
            if (colSzs != null) {
                JsonArray array = new JsonArray();
                Arrays.stream(colSzs).forEach(array::add);
                result.set("colSzs", array);
            }
            if (listenerId != null) {
                result.set("listener_id", listenerId);
            }
            if (port != null) {
                result.set("port", port);
            }
            if (portSsl != null) {
                result.set("port_ssl", portSsl);
            }
            if (reconnect != null) {
                result.set("reconnect", reconnect);
            }
            if (ip != null) {
                result.set("ip", ip);
            }
            return result.toString();
        }
    }
}

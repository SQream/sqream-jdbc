package com.sqream.jdbc.connector;

import com.eclipsesource.json.Json;
import com.eclipsesource.json.JsonArray;
import com.eclipsesource.json.JsonObject;
import com.eclipsesource.json.JsonValue;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class JsonParser {
    private static final Logger LOGGER = Logger.getLogger(JsonParser.class.getName());

    private static final String QUERY_TYPE = "queryType";
    private static final String QUERY_TYPE_NAMED = "queryTypeNamed";

    public static final int TEXT_ITEM_SIZE = 100_000;

    public ConnectionStateDto toConnectionState(String body) throws ConnException {
        JsonObject jsonObj = parseJson(body);
        int connId = safeReadValue(jsonObj, "connectionId").asInt();
        String varcharEncoding = jsonObj.getString("varcharEncoding", "ascii");
        varcharEncoding = (varcharEncoding.contains("874"))? "cp874" : "ascii";

        LOGGER.log(Level.FINEST, MessageFormat.format(
                "Parsed: connectionId = [{0}], varcharEncoding = [{1}] from json [{2}]",
                connId, varcharEncoding, body));

        return new ConnectionStateDto(connId, varcharEncoding);
    }

    public List<ColumnMetadataDto> toQueryTypeInput(String body) throws ConnException {
        LOGGER.log(Level.FINEST, body);
        return toQueryType(body, QUERY_TYPE);
    }

    public List<ColumnMetadataDto> toQueryTypeOut(String body) throws ConnException {
        LOGGER.log(Level.FINEST, body);
        return toQueryType(body, QUERY_TYPE_NAMED);
    }

    public FetchMetadataDto toFetchMetadata(String body) throws ConnException {
        JsonObject jsonObj = parseJson(body);
        int newRowsFetched = safeReadValue(jsonObj,"rows").asInt();
        JsonArray columnSizes = safeReadValue(jsonObj, "colSzs").asArray();
        int[] sizes = new int[columnSizes.size()];
        for (int i = 0; i < columnSizes.size(); i++) {
            sizes[i] = columnSizes.get(i).asInt();
        }

        if (LOGGER.getParent().getLevel() == Level.FINEST) {
            LOGGER.log(Level.FINEST, MessageFormat.format(
                    "Parsed: newRowsFetched = [{0}], sizes = [{1}]", newRowsFetched, Arrays.toString(sizes)));
        }

        return new FetchMetadataDto(newRowsFetched, sizes);
    }

    public int toStatementId(String body) throws ConnException {
        JsonObject jsonObj = parseJson(body);
        int statementId = safeReadValue(jsonObj, "statementId").asInt();
        LOGGER.log(Level.FINEST, MessageFormat.format("Statement id = [{0}] from json [{1}]", statementId, body));
        return statementId;
    }

    public StatementStateDto toStatementState(String body) throws ConnException {
        JsonObject jsonObj = parseJson(body);

        int listenerId = safeReadValue(jsonObj,"listener_id").asInt();
        int port = safeReadValue(jsonObj, "port").asInt();
        int portSsl = safeReadValue(jsonObj, "port_ssl").asInt();
        boolean reconnect = safeReadValue(jsonObj, "reconnect").asBoolean();
        String ip = safeReadValue(jsonObj, "ip").asString();

        LOGGER.log(Level.FINEST, MessageFormat.format(
                "Parsed: listenerId=[{0}], port=[{1}], portSsl=[{2}], reconnect=[{3}], ip=[{4}] from json [{5}]",
                listenerId, port, portSsl, reconnect, ip, body));

        return new StatementStateDto(listenerId, port, portSsl, reconnect, ip);
    }

    private JsonValue safeReadValue(JsonObject jsonObj, String key) throws ConnException {
        JsonValue value = jsonObj.get(key);
        if (value == null) {
            throw new ConnException(MessageFormat.format("Wrong json: [{0}]. Does not contain key: [{1}]",
                    jsonObj.toString(), key));
        }
        return value;
    }

    private List<ColumnMetadataDto> toQueryType(String body, String type) throws ConnException {
        LOGGER.log(Level.FINEST, MessageFormat.format("Json=[{0}]",body));

        List<ColumnMetadataDto> resultList = new ArrayList<>();
        JsonArray jsonArray = parseJson(body).get(type).asArray();
        for (int i = 0; i < jsonArray.size(); i++) {
            resultList.add(
                    toColumnMetadata(
                            jsonArray.get(i).toString())
            );
        }
        return resultList;
    }

    private ColumnMetadataDto toColumnMetadata(String body) throws ConnException {
        LOGGER.log(Level.FINEST, MessageFormat.format("Json=[{0}]",body));

        JsonObject jsonObj = parseJson(body);

        boolean truVarchar = jsonObj.get("isTrueVarChar").asBoolean();
        String name = jsonObj.getString("name", "");
        boolean nullable = jsonObj.get("nullable").asBoolean();
        JsonArray type = jsonObj.get("type").asArray();
        String valueType = type.get(0).asString();
        int valueSize = type.get(1).asInt() != 0 ? type.get(1).asInt() : TEXT_ITEM_SIZE;

        return new ColumnMetadataDto(truVarchar, name, nullable, valueType, valueSize);
    }

    private JsonObject parseJson(String jsonStr) throws ConnException {
        JsonObject jsonObj = Json.parse(jsonStr).asObject();
        checkError(jsonObj);
        return jsonObj;
    }

    private void checkError(JsonObject jsonObj) throws ConnException {
        if(jsonObj.get("error") != null) {
            throw new ConnException(String.format("\n\nError from SQream:\n\n%s",jsonObj.get("error").asString()));
        }
    }
}

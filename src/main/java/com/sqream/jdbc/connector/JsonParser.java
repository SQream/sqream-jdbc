package com.sqream.jdbc.connector;

import com.eclipsesource.json.Json;
import com.eclipsesource.json.JsonArray;
import com.eclipsesource.json.JsonObject;
import com.eclipsesource.json.JsonValue;

import java.util.ArrayList;
import java.util.List;

public class JsonParser {

    private static final String QUERY_TYPE = "queryType";
    private static final String QUERY_TYPE_NAMED = "queryTypeNamed";

    public ConnectionStateDto toConnectionState(String body) {
        JsonObject jsonObj = parseJson(body);
        int connId = jsonObj.get("connectionId").asInt();
        String varcharEncoding = jsonObj.getString("varcharEncoding", "ascii");
        varcharEncoding = (varcharEncoding.contains("874"))? "cp874" : "ascii";
        return new ConnectionStateDto(connId, varcharEncoding);
    }

    public List<ColumnMetadataDto> toQueryTypeInput(String body) {
        return toQueryType(body, QUERY_TYPE);
    }

    public List<ColumnMetadataDto> toQueryTypeOut(String body) {
        return toQueryType(body, QUERY_TYPE_NAMED);
    }

    public FetchMetadataDto toFetchMetadata(String body) {
        JsonObject jsonObj = parseJson(body);
        int newRowsFetched = jsonObj.get("rows").asInt();
        JsonArray jsonSizes = jsonObj.get("colSzs").asArray();
        int[] sizes = new int[jsonSizes.size()];
        for (int i = 0; i < jsonSizes.size(); i++) {
            sizes[i] = jsonSizes.get(i).asInt();
        }
        return new FetchMetadataDto(newRowsFetched, sizes);
    }

    public int toStatementId(String body) {
        return parseJson(body).get("statementId").asInt();
    }

    public StatementStateDto toStatementState(String body) throws ConnException {
        JsonObject jsonObj = parseJson(body);

        if(jsonObj.get("error") != null) {
            throw new ConnException(jsonObj.get("error").asString());
        }

        int listenerId = jsonObj.get("listener_id").asInt();
        int port = jsonObj.get("port").asInt();
        int portSsl = jsonObj.get("port_ssl").asInt();
        boolean reconnect = jsonObj.get("reconnect").asBoolean();
        String ip = jsonObj.get("ip").asString();

        return new StatementStateDto(listenerId, port, portSsl, reconnect, ip);
    }

    private List<ColumnMetadataDto> toQueryType(String body, String type) {
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

    private ColumnMetadataDto toColumnMetadata(String body) {
        JsonObject jsonObj = parseJson(body);

        boolean truVarchar = jsonObj.get("isTrueVarChar").asBoolean();
        String name = jsonObj.getString("name", "");
        boolean nullable = jsonObj.get("nullable").asBoolean();
        JsonArray type = jsonObj.get("type").asArray();
        String valueType = type.get(0).asString();
        int valueSize = type.get(1).asInt();

        return new ColumnMetadataDto(truVarchar, name, nullable, valueType, valueSize);
    }

    private JsonObject parseJson(String jsonStr) {
        return Json.parse(jsonStr).asObject();
    }
}

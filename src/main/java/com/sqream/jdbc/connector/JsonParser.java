package com.sqream.jdbc.connector;

import com.eclipsesource.json.Json;
import com.eclipsesource.json.JsonArray;
import com.eclipsesource.json.JsonObject;

import java.util.ArrayList;
import java.util.List;

public class JsonParser {

    private static final String QUERY_TYPE = "queryType";
    private static final String QUERY_TYPE_NAMED = "queryTypeNamed";

    public List<ColumnMetadataDto> toQueryTypeInput(String body) {
        return toQueryType(body, QUERY_TYPE);
    }

    public List<ColumnMetadataDto> toQueryTypeOut(String body) {
        return toQueryType(body, QUERY_TYPE_NAMED);
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

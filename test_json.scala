// To run, in this folder type:
// scala -cp ./minimal-json-0.9.5.jar test_json.scala

import com.eclipsesource.json._

// Test JSON string
val json_str = """{"queryTypeNamed":[{"isTrueVarChar":false,"name":"?column?","nullable":false,"type":["ftVarchar",128,0]}]}"""

// Loading Json value list
val json_lst = Json.parse(json_str).asObject().get("queryTypeNamed").asArray()

val is_tvc = json_lst.get(0).asObject().get("isTrueVarChar")


println(is_tvc)


Object json_object;
// json_object = Json.parse(json).asObject();

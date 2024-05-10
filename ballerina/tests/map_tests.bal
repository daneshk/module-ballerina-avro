// Copyright (c) 2024 WSO2 LLC. (http://www.wso2.com).
//
// WSO2 LLC. licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

import ballerina/test;
import ballerina/io;

@test:Config {
    groups: ["map", "bytes"]
}
public isolated function testMapsWithBytes() returns error? {
    string schema = string `
        {
            "type": "map",
            "values" : "bytes",
            "default": {}
        }`;

    map<byte[]> colors = {"red": "0".toBytes(), "green": "1".toBytes(), "blue": "2".toBytes()};
    Schema avro = check new (schema);
    byte[] serializedValue = check avro.toAvro(colors);
    map<byte[]> deserializedValue = check avro.fromAvro(serializedValue);
    test:assertEquals(colors, deserializedValue);
}

@test:Config {
    groups: ["map", "fixed"]
}
public isolated function testMapsWithFixed() returns error? {
    string schema = string `
        {
            "type": "map",
            "values" : {
                "type": "fixed",
                "name": "name",
                "size": 1
            },
            "default": {}
        }`;

    map<byte[]> colors = {"red": "0".toBytes(), "green": "1".toBytes(), "blue": "2".toBytes()};
    Schema avro = check new (schema);
    byte[] serializedValue = check avro.toAvro(colors);
    map<byte[]> deserializedValue = check avro.fromAvro(serializedValue);
    test:assertEquals(colors, deserializedValue);
}

@test:Config {
    groups: ["map", "fixed"]
}
public isolated function testMapsOfFixedMaps() returns error? {
    string schema = string `
        {
            "type": "map",
            "values" : {
                "type": "map",
                "values" : {
                    "type": "fixed",
                    "name": "name",
                    "size": 1
                },
                "default": {}
            },
            "default": {}
        }`;

    map<map<byte[]>> colors = {
        "red": {"r": "0".toBytes(), "g": "1".toBytes(), "b": "2".toBytes()},
        "green": {"r": "0".toBytes(), "g": "1".toBytes(), "b": "2".toBytes()},
        "blue": {"r": "0".toBytes(), "g": "1".toBytes(), "b": "2".toBytes()}
    };
    Schema avro = check new (schema);
    byte[] serializedValue = check avro.toAvro(colors);
    map<map<byte[]>> deserializedValue = check avro.fromAvro(serializedValue);
    test:assertEquals(colors, deserializedValue);
}

@test:Config {
    groups: ["map", "record", "kkk"]
}
public isolated function testReadOnlyMapsWithReadOnlyRecords() returns error? {
    string jsonFileName = string `tests/resources/schema_map_readonly.json`;
    string schema = (check io:fileReadJson(jsonFileName)).toString();

    map<Instructor & readonly> & readonly instructors = {
        "john": {name: "John", student: {name: "Alice", subject: "Math"}},
        "doe": {name: "Doe", student: {name: "Bob", subject: "Science"}},
        "jane": {name: "Jane", student: {name: "Charlie", subject: "English"}}
    };

    Schema avro = check new (schema);
    byte[] serializedValue = check avro.toAvro(instructors);
    map<Instructor & readonly> & readonly deserializedValue = check avro.fromAvro(serializedValue);
    test:assertEquals(instructors, deserializedValue);
}

@test:Config {
    groups: ["map", "record", "kkk"]
}
public isolated function testMapsWithRecordsWithReadOnly() returns error? {
    string jsonFileName = string `tests/resources/schema_map_records_readonly.json`;
    string schema = (check io:fileReadJson(jsonFileName)).toString();

    map<Instructor> & readonly instructors = {
        "john": {name: "John", student: {name: "Alice", subject: "Math"}},
        "doe": {name: "Doe", student: {name: "Bob", subject: "Science"}},
        "jane": {name: "Jane", student: {name: "Charlie", subject: "English"}}
    };

    Schema avro = check new (schema);
    byte[] serializedValue = check avro.toAvro(instructors);
    map<Instructor> & readonly deserializedValue = check avro.fromAvro(serializedValue);
    test:assertEquals(instructors, deserializedValue);
}

@test:Config {
    groups: ["map", "record"]
}
public isolated function testMapsWithReadOnlyRecordsWithReadOnly() returns error? {
    string jsonFileName = string `tests/resources/schema_map_readonly_records_readonly.json`;
    string schema = (check io:fileReadJson(jsonFileName)).toString();
        
    map<Instructor & readonly> & readonly instructors = {
        "john": {name: "John", student: {name: "Alice", subject: "Math"}},
        "doe": {name: "Doe", student: {name: "Bob", subject: "Science"}},
        "jane": {name: "Jane", student: {name: "Charlie", subject: "English"}}
    };

    Schema avro = check new (schema);
    byte[] serializedValue = check avro.toAvro(instructors);
    map<Instructor & readonly> & readonly deserializedValue = check avro.fromAvro(serializedValue);
    test:assertEquals(instructors, deserializedValue);
}

@test:Config {
    groups: ["map", "record"]
}
public isolated function testMapsWithReadOnlyRecords() returns error? {
    string jsonFileName = string `tests/resources/schema_map_readonly_records.json`;
    string schema = (check io:fileReadJson(jsonFileName)).toString();

    map<Instructor & readonly> instructors = {
        "john": {name: "John", student: {name: "Alice", subject: "Math"}},
        "doe": {name: "Doe", student: {name: "Bob", subject: "Science"}},
        "jane": {name: "Jane", student: {name: "Charlie", subject: "English"}}
    };

    Schema avro = check new (schema);
    byte[] serializedValue = check avro.toAvro(instructors);
    map<Instructor & readonly> deserializedValue = check avro.fromAvro(serializedValue);
    test:assertEquals(instructors, deserializedValue);
}

@test:Config {
    groups: ["map", "record"]
}
public isolated function testMapsWithRecords() returns error? {
    string jsonFileName = string `tests/resources/schema_map_records.json`;
    string schema = (check io:fileReadJson(jsonFileName)).toString();

    map<Instructor> instructors = {
        "john": {name: "John", student: {name: "Alice", subject: "Math"}},
        "doe": {name: "Doe", student: {name: "Bob", subject: "Science"}},
        "jane": {name: "Jane", student: {name: "Charlie", subject: "English"}}
    };

    Schema avro = check new (schema);
    byte[] serializedValue = check avro.toAvro(instructors);
    map<Instructor> deserializedValue = check avro.fromAvro(serializedValue);
    test:assertEquals(instructors, deserializedValue);
}

@test:Config {
    groups: ["map"]
}
public isolated function testMapsWithInt() returns error? {
    string schema = string `
        {
            "type": "map",
            "values" : "int",
            "default": {}
        }`;

    map<int> colors = {"red": 0, "green": 1, "blue": 2};

    Schema avro = check new (schema);
    byte[] serializedValue = check avro.toAvro(colors);
    map<int> deserializedValue = check avro.fromAvro(serializedValue);
    test:assertEquals(colors, deserializedValue);
}

@test:Config {
    groups: ["map", "enum", "lk"]
}
public isolated function testMapsWithEnum() returns error? {
    string schema = string `
        {
            "type": "map",
            "values": {
                "type": "enum",
                "name": "Numbers",
                "symbols": ["ONE", "TWO", "THREE"]
            },
            "default": {}
        }`;

    map<Numbers> colors = {"red": "ONE", "green": "TWO", "blue": "THREE"};

    Schema avro = check new (schema);
    byte[] serializedValue = check avro.toAvro(colors);
    map<Numbers> deserializedValue = check avro.fromAvro(serializedValue);
    test:assertEquals(colors, deserializedValue);
}

@test:Config {
    groups: ["map", "enum", "array"]
}
public isolated function testMapsWithEnumArrays() returns error? {
    string schema = string `
        {
            "type": "map",
            "values": {
                "type": "array",
                "name" : "enums", 
                "namespace": "data", 
                "items": {
                    "type": "enum",
                    "name": "Numbers",
                    "symbols": [ "ONE", "TWO", "THREE", "FOUR" ]
                }
            },
            "default": {}
        }`;

    map<Numbers[]> colors = {"red": ["ONE", "TWO", "THREE"], "green": ["ONE", "TWO", "THREE"], "blue": ["ONE", "TWO", "THREE"]};

    Schema avro = check new (schema);
    byte[] serializedValue = check avro.toAvro(colors);
    map<Numbers[]> deserializedValue = check avro.fromAvro(serializedValue);
    test:assertEquals(colors, deserializedValue);
}

@test:Config {
    groups: ["map", "float"]
}
public isolated function testMapsWithFloat() returns error? {
    string schema = string `
        {
            "type": "map",
            "values": "float",
            "default": {}
        }`;

    map<float> colors = {"red": 2.3453, "green": 435.563, "blue": 20347.23};

    Schema avro = check new (schema);
    byte[] serializedValue = check avro.toAvro(colors);
    map<float> deserializedValue = check avro.fromAvro(serializedValue);
    test:assertEquals(colors, deserializedValue);
}

@test:Config {
    groups: ["map", "double"]
}
public isolated function testMapsWithDouble() returns error? {
    string schema = string `
        {
            "type": "map",
            "values": "double",
            "default": {}
        }`;

    map<float> colors = {"red": 2.3453, "green": 435.563, "blue": 20347.23};

    Schema avro = check new (schema);
    byte[] serializedValue = check avro.toAvro(colors);
    map<float> deserializedValue = check avro.fromAvro(serializedValue);
    test:assertEquals(colors, deserializedValue);
}

@test:Config {
    groups: ["map", "double", "array"]
}
public isolated function testMapsWithDoubleArray() returns error? {
    string schema = string `
        {
            "type": "map",
            "values": {
                "type": "array",
                "name" : "enums", 
                "namespace": "data", 
                "items": "double"
            },
            "default": {}
        }`;

    map<float[]> colors = {"red": [2.3434253, 435.56433, 20347.22343], "green": [2.3452343, 435.56343, 20347.2423], "blue": [2.3453243, 435.56243, 20347.22343]};

    Schema avro = check new (schema);
    byte[] serializedValue = check avro.toAvro(colors);
    map<float[]> deserializedValue = check avro.fromAvro(serializedValue);
    test:assertEquals(colors, deserializedValue);
}

@test:Config {
    groups: ["map", "long"]
}
public isolated function testMapsWithLong() returns error? {
    string schema = string `
        {
            "type": "map",
            "values": "long",
            "default": {}
        }`;

    map<int> colors = {"red": 2, "green": 435, "blue": 2034723};

    Schema avro = check new (schema);
    byte[] serializedValue = check avro.toAvro(colors);
    map<int> deserializedValue = check avro.fromAvro(serializedValue);
    test:assertEquals(colors, deserializedValue);
}

@test:Config {
    groups: ["map", "string"]
}
public isolated function testMapsWithStrings() returns error? {
    string schema = string `
        {
            "type": "map",
            "values": "string",
            "default": {}
        }`;

    map<string> colors = {"red": "2", "green": "435", "blue": "2034723"};

    Schema avro = check new (schema);
    byte[] serializedValue = check avro.toAvro(colors);
    map<string> deserializedValue = check avro.fromAvro(serializedValue);
    test:assertEquals(colors, deserializedValue);
}

@test:Config {
    groups: ["map", "errors"]
}
public isolated function testMapsWithUnionTypes() returns error? {
    string schema = string `
        {
            "type": "map",
            "values": ["string", "int"],
            "default": {}
        }`;

    map<int|string> colors = {"red": "2", "green": "435", "blue": "2034723"};

    Schema avro = check new (schema);
    byte[]|Error serializedValue = avro.toAvro(colors);
    test:assertTrue(serializedValue is Error);
}

@test:Config {
    groups: ["map", "boolean", "ssq"]
}
public isolated function testMapsWithBoolean() returns error? {
    string schema = string `
        {
            "type": "map",
            "values": "boolean",
            "default": {}
        }`;

    map<boolean> colors = {"red": true, "green": false, "blue": false};

    Schema avro = check new (schema);
    byte[] serializedValue = check avro.toAvro(colors);
    map<boolean> deserializedValue = check avro.fromAvro(serializedValue);
    test:assertEquals(colors, deserializedValue);
}

@test:Config {
    groups: ["map", "boolean", "ssq"]
}
public isolated function testMapsWithBooleanWithReadOnlyValues() returns error? {
    string schema = string `
        {
            "type": "map",
            "values": "boolean",
            "default": {}
        }`;

    map<boolean> & readonly colors = {"red": true, "green": false, "blue": false};

    Schema avro = check new (schema);
    byte[] serializedValue = check avro.toAvro(colors);
    map<boolean> & readonly deserializedValue = check avro.fromAvro(serializedValue);
    test:assertEquals(colors, deserializedValue);
}

@test:Config {
    groups: ["map"]
}
public isolated function testMapsWithMaps() returns error? {
    string schema = string `
        {
            "type": "map",
            "values": {
                "type": "map",
                "values": "long"
            },
            "default": {}
        }`;

    map<map<int>> colors = {
        "red": {"r": 2, "g": 3, "b": 4},
        "green": {"r": 5, "g": 6, "b": 7},
        "blue": {"r": 8, "g": 9, "b": 10}
    };

    Schema avro = check new (schema);
    byte[] serializedValue = check avro.toAvro(colors);
    map<map<int>> deserializedValue = check avro.fromAvro(serializedValue);
    test:assertEquals(colors, deserializedValue);
}

@test:Config {
    groups: ["map", "k"]
}
public isolated function testMapsWithNestedMapsWithReadOnlyValues() returns error? {
    string schema = string `
        {
            "type": "map",
            "values": {
                "type": "map",
                "values": {
                    "type": "map",
                    "values": "int"
                }
            },
            "default": {}
        }`;

    map<map<map<int>>> & readonly colors = {
        "red": {"r": {"r": 2, "g": 3, "b": 4}, "g": {"r": 5, "g": 6, "b": 7}, "b": {"r": 8, "g": 9, "b": 10}},
        "green": {"r": {"r": 2, "g": 3, "b": 4}, "g": {"r": 5, "g": 6, "b": 7}, "b": {"r": 8, "g": 9, "b": 10}},
        "blue": {"r": {"r": 2, "g": 3, "b": 4}, "g": {"r": 5, "g": 6, "b": 7}, "b": {"r": 8, "g": 9, "b": 10}}
    };

    Schema avro = check new (schema);
    byte[] serializedValue = check avro.toAvro(colors);
    map<map<map<int>>> & readonly deserializedValue = check avro.fromAvro(serializedValue);
    test:assertEquals(colors, deserializedValue);
}

@test:Config {
    groups: ["map", "k"]
}
public isolated function testMapsWithNestedMaps() returns error? {
    string schema = string `
        {
            "type": "map",
            "values": {
                "type": "map",
                "values": {
                    "type": "map",
                    "values": "int"
                }
            },
            "default": {}
        }`;

    map<map<map<int>>> colors = {
        "red": {"r": {"r": 2, "g": 3, "b": 4}, "g": {"r": 5, "g": 6, "b": 7}, "b": {"r": 8, "g": 9, "b": 10}},
        "green": {"r": {"r": 2, "g": 3, "b": 4}, "g": {"r": 5, "g": 6, "b": 7}, "b": {"r": 8, "g": 9, "b": 10}},
        "blue": {"r": {"r": 2, "g": 3, "b": 4}, "g": {"r": 5, "g": 6, "b": 7}, "b": {"r": 8, "g": 9, "b": 10}}
    };

    Schema avro = check new (schema);
    byte[] serializedValue = check avro.toAvro(colors);
    map<map<map<int>>> deserializedValue = check avro.fromAvro(serializedValue);
    test:assertEquals(colors, deserializedValue);
}

@test:Config {
    groups: ["map", "long", "qwe"]
}
public isolated function testMapsWithLongArray() returns error? {
    string schema = string `
        {
            "type": "map",
            "values": {
                "type": "array",
                "items": "long"
            },
            "default": {}
        }`;

    map<int[]> colors = {"red": [252, 122, 41], "green": [235, 163, 23], "blue": [207, 123]};

    Schema avro = check new (schema);
    byte[] serializedValue = check avro.toAvro(colors);
    map<int[]> deserializedValue = check avro.fromAvro(serializedValue);
    test:assertEquals(deserializedValue, colors);
}

@test:Config {
    groups: ["map", "int", "az"]
}
public isolated function testMapsWithIntArray() returns error? {
    string schema = string `
        {
            "type": "map",
            "values": {
                "type": "array",
                "items": "int"
            },
            "default": {}
        }`;

    map<int[]> colors = {"red": [252, 122, 41], "green": [235, 163, 23], "blue": [207, 123]};

    Schema avro = check new (schema);
    byte[] serializedValue = check avro.toAvro(colors);
    map<int[]> deserializedValue = check avro.fromAvro(serializedValue);
    test:assertEquals(deserializedValue, colors);
}

@test:Config {
    groups: ["map", "int", "az"]
}
public isolated function testMapsWithIntArrayWithReadOnlyValues() returns error? {
    string schema = string `
        {
            "type": "map",
            "values": {
                "type": "array",
                "items": "int"
            },
            "default": {}
        }`;

    map<int[]> & readonly colors = {"red": [252, 122, 41], "green": [235, 163, 23], "blue": [207, 123]};

    Schema avro = check new (schema);
    byte[] serializedValue = check avro.toAvro(colors);
    map<int[]> & readonly deserializedValue = check avro.fromAvro(serializedValue);
    test:assertEquals(deserializedValue, colors);
}

@test:Config {
    groups: ["map", "float"]
}
public isolated function testMapsWithFloatArray() returns error? {
    string schema = string `
        {
            "type": "map",
            "values": {
                "type": "array",
                "items": "float"
            },
            "default": {}
        }`;

    map<float[]> colors = {"red": [252.32, 122.45, 41.342], "green": [235.321, 163.3, 23.324], "blue": [207.23434, 123.23]};

    Schema avro = check new (schema);
    byte[] serializedValue = check avro.toAvro(colors);
    map<float[]> deserializedValue = check avro.fromAvro(serializedValue);
    test:assertEquals(deserializedValue, colors);
}

@test:Config {
    groups: ["map", "string"]
}
public isolated function testMapsWithStringArray() returns error? {
    string schema = string `
        {
            "type": "map",
            "values": {
                "type": "array",
                "items": "string"
            },
            "default": {}
        }`;

    map<string[]> colors = {"red": ["252", "122", "41"], "green": ["235", "163", "23"], "blue": ["207", "123"]};

    Schema avro = check new (schema);
    byte[] serializedValue = check avro.toAvro(colors);
    map<string[]> deserializedValue = check avro.fromAvro(serializedValue);
    test:assertEquals(deserializedValue, colors);
}

@test:Config {
    groups: ["map", "string", "null", "union", "sbs"]
}
public isolated function testMapsWithUnionArray() returns error? {
    string schema = string `
        {
            "type": "map",
            "values": {
                "type": "array",
                "items": ["string", "null"]
            },
            "default": {}
        }`;

    map<string[]> colors = {"red": ["252", "122", "41"], "green": ["235", "163", "23"], "blue": ["207", "123"]};

    Schema avro = check new (schema);
    byte[] serializedValue = check avro.toAvro(colors);
    map<string[]> deserializedValue = check avro.fromAvro(serializedValue);
    test:assertEquals(deserializedValue, colors);
}

@test:Config {
    groups: ["map", "int", "null", "union"]
}
public isolated function testMapsWithUnionIntArray() returns error? {
    string schema = string `
        {
            "type": "map",
            "values": {
                "type": "array",
                "items": ["int", "null"]
            },
            "default": {}
        }`;

    map<int[]> colors = {"red": [252, 122, 41], "green": [235, 163, 23], "blue": [207, 123]};

    Schema avro = check new (schema);
    byte[] serializedValue = check avro.toAvro(colors);
    map<int[]> deserializedValue = check avro.fromAvro(serializedValue);
    test:assertEquals(deserializedValue, colors);
}

@test:Config {
    groups: ["map", "long", "null", "union", "sxs"]
}
public isolated function testMapsWithUnionLongArray() returns error? {
    string schema = string `
        {
            "type": "map",
            "values": {
                "type": "array",
                "items": ["long", "null"]
            },
            "default": {}
        }`;

    map<int[]> colors = {"red": [252, 122, 41], "green": [235, 163, 23], "blue": [207, 123]};

    Schema avro = check new (schema);
    byte[] serializedValue = check avro.toAvro(colors);
    map<int[]> deserializedValue = check avro.fromAvro(serializedValue);
    test:assertEquals(deserializedValue, colors);
}

@test:Config {
    groups: ["map", "float", "null", "union", "sss"]
}
public isolated function testMapsWithUnionFloatArray() returns error? {
    string schema = string `
        {
            "type": "map",
            "values": {
                "type": "array",
                "items": ["float", "null"]
            },
            "default": {}
        }`;

    map<float[]> colors = {"red": [252.32, 122.45, 41.342], "green": [235.321, 163.3, 23.324], "blue": [207.23434, 123.23]};

    Schema avro = check new (schema);
    byte[] serializedValue = check avro.toAvro(colors);
    map<float[]> deserializedValue = check avro.fromAvro(serializedValue);
    test:assertEquals(deserializedValue, colors);
}

@test:Config {
    groups: ["map", "float", "null", "union"]
}
public isolated function testMapsWithUnionDoubleArray() returns error? {
    string schema = string `
        {
            "type": "map",
            "values": {
                "type": "array",
                "items": ["double", "null"]
            },
            "default": {}
        }`;

    map<float[]> colors = {"red": [252.32, 122.45, 41.342], "green": [235.321, 163.3, 23.324], "blue": [207.23434, 123.23]};

    Schema avro = check new (schema);
    byte[] serializedValue = check avro.toAvro(colors);
    map<float[]> deserializedValue = check avro.fromAvro(serializedValue);
    test:assertEquals(deserializedValue, colors);
}

@test:Config {
    groups: ["map", "bytes"]
}
public isolated function testMapsWithBytesArray() returns error? {
    string schema = string `
        {
            "type": "map",
            "values": {
                "type": "array",
                "items": "bytes"
            },
            "default": {}
        }`;

    map<byte[][]> colors = {"red": ["252".toBytes(), "122".toBytes(), "41".toBytes()], "green": ["235".toBytes(), "163".toBytes(), "23".toBytes()], "blue": ["207".toBytes(), "123".toBytes()]};

    Schema avro = check new (schema);
    byte[] serializedValue = check avro.toAvro(colors);
    map<byte[][]> deserializedValue = check avro.fromAvro(serializedValue);
    test:assertEquals(deserializedValue, colors);
}

@test:Config {
    groups: ["map", "bytes"]
}
public isolated function testMapsWithFixedArray() returns error? {
    string schema = string `
        {
            "type": "map",
            "values": {
                "type": "array",
                "items": {
                    "type": "fixed",
                    "name": "name",
                    "size": 3
                }
            },
            "default": {}
        }`;

    map<byte[][]> colors = {"red": ["252".toBytes(), "122".toBytes(), "411".toBytes()], "green": ["235".toBytes(), "163".toBytes(), "213".toBytes()], "blue": ["207".toBytes(), "123".toBytes()]};

    Schema avro = check new (schema);
    byte[] serializedValue = check avro.toAvro(colors);
    map<byte[][]> deserializedValue = check avro.fromAvro(serializedValue);
    test:assertEquals(deserializedValue, colors);
}

@test:Config {
    groups: ["map", "boolean"]
}
public isolated function testMapsWithBooleanArray() returns error? {
    string schema = string `
        {
            "type": "map",
            "values": {
                "type": "array",
                "items": "boolean"
            },
            "default": {}
        }`;

    map<boolean[]> colors = {"red": [true, false, true], "green": [false, true, false], "blue": [true, false]};

    Schema avro = check new (schema);
    byte[] serializedValue = check avro.toAvro(colors);
    map<boolean[]> deserializedValue = check avro.fromAvro(serializedValue);
    test:assertEquals(deserializedValue, colors);
}

@test:Config {
    groups: ["map", "record"]
}
public isolated function testMapsWithArrayOfRecordArray() returns error? {
    string jsonFileName = string `tests/resources/schema_map_array_record_arrays.json`;
    string schema = (check io:fileReadJson(jsonFileName)).toString();

    map<Instructor[][]> instructors = {
        "john": [[{name: "John", student: {name: "Alice", subject: "Math"}}, {name: "John", student: {name: "Alice", subject: "Math"}}]],
        "doe": [[{name: "Doe", student: {name: "Bob", subject: "Science"}}, {name: "Doe", student: {name: "Bob", subject: "Science"}}]],
        "jane": [[{name: "Jane", student: {name: "Charlie", subject: "English"}}, {name: "Jane", student: {name: "Charlie", subject: "English"}}]]
    };

    Schema avro = check new (schema);
    byte[] serializedValue = check avro.toAvro(instructors);
    map<Instructor[][]> deserializedValue = check avro.fromAvro(serializedValue);
    test:assertEquals(instructors, deserializedValue);
}

@test:Config {
    groups: ["map", "record"]
}
public isolated function testArrayOfStringArrayMaps() returns error? {
    string schema = string `
    {
        "type": "map",
        "values": {
            "type": "array",
            "name" : "stringArray", 
            "namespace": "data", 
            "items": {
                "type": "array",
                "name" : "strings", 
                "namespace": "data", 
                "items": "string"
            }
        }
    }`;

    map<string[][]> colors = {
        "red": [["red", "green", "blue"], ["red", "green", "blue"], ["red", "green", "blue"]],
        "green": [["red", "green", "blue"], ["red", "green", "blue"], ["red", "green", "blue"]],
        "blue": [["red", "green", "blue"], ["red", "green", "blue"], ["red", "green", "blue"]]
    };

    Schema avro = check new (schema);
    byte[] serializedValue = check avro.toAvro(colors);
    map<string[][]> deserializedValue = check avro.fromAvro(serializedValue);
    test:assertEquals(deserializedValue, colors);
}

@test:Config {
    groups: ["map", "record"]
}
public isolated function testMapsWithNestedRecordMaps() returns error? {
    string jsonFileName = string `tests/resources/schema_map_record_maps.json`;
    string schema = (check io:fileReadJson(jsonFileName)).toString();

    Lecturer lec = {
        name: "John",
        instructor: {
            name: "Jane",
            student: {
                name: "Charlie",
                subject: "English"
            }
        }
    };

    map<map<Lecturer>> lecturers = {
        "john": {"john": lec, "doe": lec},
        "doe": {"john": lec, "doe": lec},
        "jane": {"john": lec, "doe": lec}
    };

    Schema avro = check new (schema);
    byte[] serializedValue = check avro.toAvro(lecturers);
    map<map<Lecturer>> deserializedValue = check avro.fromAvro(serializedValue);
    test:assertEquals(lecturers, deserializedValue);
}

@test:Config {
    groups: ["map", "record"]
}
public isolated function testMapsWithNestedRecordArrayMaps() returns error? {
    string jsonFileName = string `tests/resources/schema_map_record_array.json`;
    string schema = (check io:fileReadJson(jsonFileName)).toString();

    Lecturer[] lecs = [{name: "John", instructor: {name: "Jane", student: {name: "Charlie", subject: "English"}}},
                            {name: "Doe", instructor: {name: "Jane", student: {name: "Charlie", subject: "English"}}}];

    map<map<Lecturer[]>> lecturers = {
        "john": {"r": lecs, "g": lecs, "b": lecs},
        "doe": {"r": lecs, "g": lecs, "b": lecs},
        "jane": {"r": lecs, "g": lecs, "b": lecs}
    };

    Schema avro = check new (schema);
    byte[] serializedValue = check avro.toAvro(lecturers);
    map<map<Lecturer[]>> deserializedValue = check avro.fromAvro(serializedValue);
    test:assertEquals(lecturers, deserializedValue);
}

@test:Config {
    groups: ["map", "record"]
}
public isolated function testMapsWithRecordArray() returns error? {
    string jsonFileName = string `tests/resources/schema_map_array_record.json`;
    string schema = (check io:fileReadJson(jsonFileName)).toString();

    map<Instructor[]> instructors = {
        "john": [{name: "John", student: {name: "Alice", subject: "Math"}}, {name: "John", student: {name: "Alice", subject: "Math"}}],
        "doe": [{name: "Doe", student: {name: "Bob", subject: "Science"}}, {name: "Doe", student: {name: "Bob", subject: "Science"}}],
        "jane": [{name: "Jane", student: {name: "Charlie", subject: "English"}}, {name: "Jane", student: {name: "Charlie", subject: "English"}}]
    };

    Schema avro = check new (schema);
    byte[] serializedValue = check avro.toAvro(instructors);
    map<Instructor[]> deserializedValue = check avro.fromAvro(serializedValue);
    test:assertEquals(instructors, deserializedValue);
}

@test:Config {
    groups: ["map", "record"]
}
public isolated function testMapsWithNestedRecordArrayReadOnlyMaps() returns error? {
    string jsonFileName = string `tests/resources/schema_map_record_array_readonly.json`;
    string schema = (check io:fileReadJson(jsonFileName)).toString();

    Lecturer[] lecs = [{name: "John", instructor: {name: "Jane", student: {name: "Charlie", subject: "English"}}},
                            {name: "Doe", instructor: {name: "Jane", student: {name: "Charlie", subject: "English"}}}];

    map<Lecturer[]> mapValue = {"r": lecs, "g": lecs, "b": lecs};
    map<map<Lecturer[]>> & readonly lecturers = {
        "john": mapValue.cloneReadOnly(),
        "doe": mapValue.cloneReadOnly(),
        "jane": mapValue.cloneReadOnly()
    };

    Schema avro = check new (schema);
    byte[] serializedValue = check avro.toAvro(lecturers);
    map<map<Lecturer[]>> & readonly deserializedValue = check avro.fromAvro(serializedValue);
    test:assertEquals(lecturers, deserializedValue);
}

// @test:Config {
//     groups: ["map", "record"]
// }
// public isolated function testMapsWithReadOnlyRecordArrayReadOnlyMaps() returns error? {
//     string schema = string `
//     {
//         "type": "map",
//         "values": {
//             "type": "map",
//             "values": {
//                 "type": "array",
//                 "items": {
//                     "type": "record",
//                     "name": "Lecturer",
//                     "fields": [
//                         {
//                             "name": "name",
//                             "type": ["null", "string"]
//                         },
//                         {
//                             "name": "instructor",
//                             "type": ["null", {
//                                 "type": "record",
//                                 "name": "Instructor",
//                                 "fields": [
//                                     {
//                                         "name": "name",
//                                         "type": ["null", "string"]
//                                     },
//                                     {
//                                         "name": "student",
//                                         "type": ["null", {
//                                             "type": "record",
//                                             "name": "Student",
//                                             "fields": [
//                                                 {
//                                                     "name": "name",
//                                                     "type": ["null", "string"]
//                                                 },
//                                                 {
//                                                     "name": "subject",
//                                                     "type": ["null", "string"]
//                                                 }
//                                             ]
//                                         }]
//                                     }
//                                 ]
//                             }]
//                         }
//                     ]
//                 }
//             }
//         }
//     }`;


//     Lecturer[] & readonly lecs = [{name: "John", instructor: {name: "Jane", student: {name: "Charlie", subject: "English"}}},
//                             {name: "Doe", instructor: {name: "Jane", student: {name: "Charlie", subject: "English"}}}];

//     map<Lecturer[] & readonly> mapValue = {"r": lecs, "g": lecs, "b": lecs};
//     map<map<Lecturer[] & readonly>> lecturers = {
//         "john": mapValue,
//         "doe": mapValue,
//         "jane": mapValue
//     };

//     Schema avro = check new (schema);
//     byte[] serializedValue = check avro.toAvro(lecturers);
//     map<map<Lecturer[] & readonly>> deserializedValue = check avro.fromAvro(serializedValue);
//     test:assertEquals(lecturers, deserializedValue);
// }

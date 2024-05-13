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
    groups: ["array", "int"]
}
public isolated function testIntArrays() returns error? {
    string schema = string `
        {
            "type": "array",
            "name" : "integers", 
            "namespace": "data", 
            "items": "int"
        }`;

    int[] numbers = [22, 556, 78];
    return verifyOperation(IntArray, numbers, schema);
}

@test:Config {
    groups: ["array", "int", "qqq"]
}
public isolated function testReadOnlyIntArrays() returns error? {
    string schema = string `
        {
            "type": "array",
            "name" : "integers", 
            "namespace": "data", 
            "items": "int"
        }`;

    int[] & readonly numbers = [22, 556, 78];
    return verifyOperation(ReadOnlyIntArray, numbers, schema);
}

@test:Config {
    groups: ["array", "string"]
}
public isolated function testStringArrays() returns error? {
    string schema = string `
        {
            "type": "array",
            "name" : "stringArray", 
            "namespace": "data", 
            "items": "string"
        }`;

    string[] colors = ["red", "green", "blue"];
    return verifyOperation(StringArray, colors, schema);
}

@test:Config {
    groups: ["array", "string"]
}
public isolated function testReadOnlyStringArrays() returns error? {
    string schema = string `
        {
            "type": "array",
            "name" : "stringArray", 
            "namespace": "data", 
            "items": "string"
        }`;

    ReadOnlyStringArray colors = ["red", "green", "blue"];
    return verifyOperation(ReadOnlyStringArray, colors, schema);
}

@test:Config {
    groups: ["array", "string"]
}
public isolated function testArrayOfStringArrays() returns error? {
    string schema = string `
        {
            "type": "array",
            "name" : "stringArray", 
            "namespace": "data", 
            "items": {
                "type": "array",
                "name" : "strings", 
                "namespace": "data", 
                "items": "string"
            }
        }`;

    string[][] colors = [["red", "green", "blue"]];
    return verifyOperation(String2DArray, colors, schema);
}

@test:Config {
    groups: ["array", "string", "mn"]
}
public isolated function testReadOnlyArrayOfStringArrays() returns error? {
    string schema = string `
        {
            "type": "array",
            "name" : "stringArray", 
            "namespace": "data", 
            "items": {
                "type": "array",
                "name" : "strings", 
                "namespace": "data", 
                "items": "string"
            }
        }`;

    string[][] & readonly colors = [["red", "green", "blue"]];
    return verifyOperation(ReadOnlyString2DArray, colors, schema);
}

@test:Config {
    groups: ["array", "string", "enn"]
}
public isolated function testEnumArrays() returns error? {
    string schema = string `
        {
            "type": "array",
            "name" : "enums", 
            "namespace": "data", 
            "items": {
                "type": "enum",
                "name": "Numbers",
                "symbols": [ "ONE", "TWO", "THREE", "FOUR" ]
            }
        }`;

    Numbers[] colors = ["ONE", "TWO", "THREE"];
    return verifyOperation(EnumArray, colors, schema);
}

@test:Config {
    groups: ["array", "string"]
}
public isolated function testArrayOfEnumArrays() returns error? {
    string schema = string `
        {
            "type": "array",
            "name" : "enums", 
            "namespace": "data", 
            "items": {
                "type": "array",
                "name" : "enumsValues", 
                "namespace": "data", 
                "items": {
                    "type": "enum",
                    "name": "Numbers",
                    "symbols": [ "ONE", "TWO", "THREE", "FOUR" ]
                }
            }
        }`;

    Numbers[][] colors = [["ONE", "TWO", "THREE"], ["ONE", "TWO", "THREE"]];
    return verifyOperation(Enum2DArray, colors, schema);
}

@test:Config {
    groups: ["array", "float"]
}
public isolated function testFloatArrays() returns error? {
    string schema = string `
        {
            "type": "array",
            "name" : "floatArray", 
            "namespace": "data", 
            "items": "float"
        }`;

    float[] numbers = [22.4, 556.84350, 78.0327];
    return verifyOperation(FloatArray, numbers, schema);
}

@test:Config {
    groups: ["array", "double"]
}
public isolated function testDoubleArrays() returns error? {
    string schema = string `
        {
            "type": "array",
            "name" : "doubleArray", 
            "namespace": "data", 
            "items": "double"
        }`;

    float[] numbers = [22.439475948, 556.843549485340, 78.032985693457];
    return verifyOperation(FloatArray, numbers, schema);
}

@test:Config {
    groups: ["array", "long"]
}
public isolated function testLongArrays() returns error? {
    string schema = string `
        {
            "type": "array",
            "name" : "longArray", 
            "namespace": "data", 
            "items": "long"
        }`;

    int[] numbers = [223432, 55423326, 7823423];
    return verifyOperation(IntArray, numbers, schema);
}

@test:Config {
    groups: ["array", "errors"]
}
public isolated function testInvalidDecimalArrays() returns error? {
    string schema = string `
        {
            "type": "array",
            "name" : "decimalArray", 
            "namespace": "data", 
            "items": "double"
        }`;

    decimal[] numbers = [22.439475948, 556.843549485340, 78.032985693457];

    Schema avro = check new (schema);
    byte[]|Error serializedValue = avro.toAvro(numbers);
    test:assertTrue(serializedValue is Error);
}

@test:Config {
    groups: ["array", "boolean"]
}
public isolated function testBooleanArrays() returns error? {
    string schema = string `
        {
            "type": "array",
            "name" : "booleanArray", 
            "namespace": "data", 
            "items": "boolean"
        }`;

    boolean[] numbers = [true, true, false];
    return verifyOperation(BooleanArray, numbers, schema);
}

@test:Config {
    groups: ["array", "error", "anydata"]
}
public isolated function testArraysWithAnydata() returns error? {
    string schema = string `
        {
            "type": "array",
            "name" : "floatArray", 
            "namespace": "data", 
            "items": "bytes"
        }`;

    anydata numbers = ["22.4".toBytes(), "556.84350", 78.0327];
    Schema avro = check new (schema);
    byte[]|Error serializedValue = avro.toAvro(numbers);
    test:assertTrue(serializedValue is Error);
}

@test:Config {
    groups: ["array", "byte"]
}
public isolated function testArraysWithFixed() returns error? {
    string schema = string `
        {
            "type": "array",
            "name" : "fixedArray", 
            "namespace": "data", 
            "items": {
                "type": "fixed",
                "name": "FixedBytes",
                "size": 2
            }
        }`;

    byte[][] numbers = ["22".toBytes(), "55".toBytes(), "78".toBytes()];
    return verifyOperation(ArrayOfByteArray, numbers, schema);
}

@test:Config {
    groups: ["record", "array"]
}
public isolated function testRecordsInArrays() returns error? {
    string jsonFileName = string `tests/resources/schema_array_records.json`;
    string schema = (check io:fileReadJson(jsonFileName)).toString();

    Student[] students = [{
        name: "Liam",
        subject: "geology"
    }, {
        name: "John",
        subject: "math"
    }];
    return verifyOperation(StudentArray, students, schema);
}

@test:Config {
    groups: ["record", "array"]
}
public isolated function testRecordsInReadOnlyArrays() returns error? {
    string jsonFileName = string `tests/resources/schema_array_readonly_records.json`;
    string schema = (check io:fileReadJson(jsonFileName)).toString();

    Student[] & readonly students = [{
        name: "Liam",
        subject: "geology"
    }, {
        name: "John",
        subject: "math"
    }];
    return verifyOperation(ReadOnlyStudentArray, students, schema);
}

@test:Config {
    groups: ["record", "array"]
}
public isolated function testRecordArraysInArrays() returns error? {
    string jsonFileName = string `tests/resources/schema_array_record_arrays.json`;
    string schema = (check io:fileReadJson(jsonFileName)).toString();

    Student[][] students = [[{
        name: "Liam",
        subject: "geology"
    }, {
        name: "John",
        subject: "math"
    }], [{
        name: "Liam",
        subject: "geology"
    }, {
        name: "John",
        subject: "math"
    }]];
    return verifyOperation(Student2DArray, students, schema);
}

@test:Config {
    groups: ["record", "array"]
}
public isolated function testRecordArraysInReadOnlyArrays() returns error? {
    string jsonFileName = string `tests/resources/schema_array_readonly_arrays.json`;
    string schema = (check io:fileReadJson(jsonFileName)).toString();

    Student[][] & readonly students = [[{
        name: "Liam",
        subject: "geology"
    }, {
        name: "John",
        subject: "math"
    }], [{
        name: "Liam",
        subject: "geology"
    }, {
        name: "John",
        subject: "math"
    }]];

    return verifyOperation(ReadOnlyStudent2DArray, students, schema);
}

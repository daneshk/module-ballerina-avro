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

@test:Config{}
public isolated function testEnums() returns error? {
    string schema = string `
        {
            "type" : "enum",
            "name" : "Numbers", 
            "namespace": "data", 
            "symbols" : [ "ONE", "TWO", "THREE", "FOUR" ]
        }`;

    Numbers number = "ONE";

    Avro avro = check new(schema);
    byte[] encode = check avro.toAvro(number);
    Numbers deserialize = check avro.fromAvro(encode);
    test:assertEquals(number, deserialize);
}

@test:Config{}
public isolated function testMaps() returns error? {
    string schema = string `
        {
            "type": "map",
            "values" : "int",
            "default": {}
        }`;

    map<int> colors = {"red": 0, "green": 1, "blue": 2};

    Avro avro = check new(schema);
    byte[] encode = check avro.toAvro(colors);
    map<int> deserialize = check avro.fromAvro(encode);
    test:assertEquals(colors, deserialize);
}

@test:Config{}
public isolated function testNestedRecords() returns error? {
    string schema = string `
    {
            "namespace": "example.avro",
            "type": "record",
            "name": "Lecturer",
            "fields": [
                {
                    "name": "name",
                    "type": "string"
                },
                {
                    "name": "instructor",
                    "type": {
                        "name": "Instructor",
                        "type": "record",
                        "fields": [
                            {
                                "name": "name",
                                "type": "string"
                            },
                            {
                                "name": "student",
                                "type": {
                                    "type": "record",
                                    "name": "Student",
                                    "fields": [
                                        {
                                            "name": "name",
                                            "type": "string"
                                        },
                                        {
                                            "name": "subject",
                                            "type": "string"
                                        }
                                    ]
                                }
                            }
                        ]
                    }
                }
            ]
        }`;

    Lecturer lecturer = {
        name: "John",
        instructor: {
            name: "Liam",
            student: {
                name: "Sam",
                subject: "geology"
            }
        }
    };

    Avro avro = check new(schema);
    byte[] serialize = check avro.toAvro(lecturer);
    Lecturer deserialize = check avro.fromAvro(serialize);
    test:assertEquals(lecturer, deserialize);
}

@test:Config{}
public isolated function testArraysInRecords() returns error? {
    string schema = string `
        {
            "namespace": "example.avro",
            "type": "record",
            "name": "Student",
            "fields": [
                {"name": "name", "type": "string"},
                {"name": "colors", "type": {"type": "array", "items": "string"}}
            ]
        }`;

    Color colors = {
        name: "Red",
        colors:  ["maroon", "dark red", "light red"]
    };

    Avro avro = check new(schema);
    byte[] serialize = check avro.toAvro(colors);
    Color deserialize = check avro.fromAvro(serialize);
    test:assertEquals(colors, deserialize);
}

@test:Config{}
public isolated function testRecords() returns error? {
    string schema = string `
        {
            "namespace": "example.avro",
            "type": "record",
            "name": "Student",
            "fields": [
                {"name": "name", "type": "string"},
                {"name": "subject", "type": "string"}
            ]
        }`;

    Student student = {
        name: "Liam",
        subject: "geology"
    };

    Avro avro = check new(schema);
    byte[] serialize = check avro.toAvro(student);
    Student deserialize = check avro.fromAvro(serialize);
    test:assertEquals(student, deserialize);
}

@test:Config{}
public isolated function testRecordsWithDifferentTypeOfFields() returns error? {
    string schema = string `
        {
            "namespace": "example.avro",
            "type": "record",
            "name": "Student",
            "fields": [
                {"name": "name", "type": "string"},
                {"name": "age", "type": "int"}
            ]
        }`;

    Person student = {
        name: "Liam",
        age: 52
    };

    Avro avro = check new(schema);
    byte[] encode = check avro.toAvro(student);
    Person deserialize = check avro.fromAvro(encode);
    test:assertEquals(student, deserialize);
}

@test:Config{}
public isolated function testRecordsWithUnionTypes() returns error? {
    string schema = string `
        {
            "namespace": "example.avro",
            "type": "record",
            "name": "Course",
            "fields": [
                {"name": "name", "type": ["string", "null"]},
                {"name": "credits", "type": ["int", "null"]}
            ]
        }`;

    Course course = {
        name: (),
        credits: ()
    };

    Avro avro = check new(schema);
    byte[] serialize = check avro.toAvro(course);
    Course deserialize = check avro.fromAvro(serialize);
    test:assertEquals(course, deserialize);
}

@test:Config{}
public isolated function testArrays() returns error? {
    string schema = string `
        {
            "type": "array",
            "name" : "StringArray", 
            "namespace": "data", 
            "items": "string"
        }`;
    
    string[] colors = ["red", "green", "blue"];

    Avro avro = check new(schema);
    byte[] encode = check avro.toAvro(colors);
    string[] deserializeJson = check avro.fromAvro(encode);
    test:assertEquals(deserializeJson, colors);
}

@test:Config{}
public isolated function testIntValue() returns error? {

    string schema = string `
        {
            "type": "int",
            "name" : "intValue", 
            "namespace": "data"
        }`;
    
    int value = 5;

    Avro avro = check new(schema);
    byte[] encode = check avro.toAvro(value);
    int deserializeJson = check avro.fromAvro(encode);
    test:assertEquals(deserializeJson, value);
}

@test:Config{}
public isolated function testFloatValue() returns error? {

    string schema = string `
        {
            "type": "float",
            "name" : "floatValue", 
            "namespace": "data"
        }`;
    
    float value = 5.5;

    Avro avro = check new(schema);
    byte[] encode = check avro.toAvro(value);
    float deserializeJson = check avro.fromAvro(encode);
    test:assertEquals(deserializeJson, value);
}

@test:Config{}
public isolated function testDoubleValue() returns error? {

    string schema = string `
        {
            "type": "double",
            "name" : "doubleValue", 
            "namespace": "data"
        }`;
    
    float value = 5.5595;

    Avro avro = check new(schema);
    byte[] encode = check avro.toAvro(value);
    float deserializeJson = check avro.fromAvro(encode);
    test:assertEquals(deserializeJson, value);
}

@test:Config{}
public isolated function testLongValue() returns error? {

    string schema = string `
        {
            "type": "long",
            "name" : "longValue", 
            "namespace": "data"
        }`;
    
    int value = 555950000000000000;

    Avro avro = check new(schema);
    byte[] encode = check avro.toAvro(value);
    int deserializeJson = check avro.fromAvro(encode);
    test:assertEquals(deserializeJson, value);
}

@test:Config{}
public isolated function testStringValue() returns error? {

    string schema = string `
        {
            "type": "string",
            "name" : "stringValue", 
            "namespace": "data"
        }`;
    
    string value = "test";

    Avro avro = check new(schema);
    byte[] encode = check avro.toAvro(value);
    string deserializeJson = check avro.fromAvro(encode);
    test:assertEquals(deserializeJson, value);
}

@test:Config{}
public isolated function testBoolean() returns error? {

    string schema = string `
        {
            "type": "boolean",
            "name" : "booleanValue", 
            "namespace": "data"
        }`;
    
    boolean value = true;

    Avro avro = check new(schema);
    byte[] encode = check avro.toAvro(value);
    boolean deserializeJson = check avro.fromAvro(encode);
    test:assertEquals(deserializeJson, value);
}

@test:Config{}
public isolated function testNullValues() returns error? {

    string schema = string `
        {
            "type": "null",
            "name" : "nullValue", 
            "namespace": "data"
        }`;

    Avro avro = check new(schema);
    byte[] encode = check avro.toAvro(());
    () deserializeJson = check avro.fromAvro(encode);
    test:assertEquals(deserializeJson, ());
}

@test:Config{}
public isolated function testFixed() returns error? {

    string schema = string `
        {
            "type": "fixed",
            "name": "name",
            "size": 16
        }`;

    byte[] value = "u00ffffffffffffx".toBytes();

    Avro avro = check new(schema);
    byte[] encode = check avro.toAvro(value);
    byte[] deserialize = check avro.fromAvro(encode);
    test:assertEquals(deserialize, value);
}

// @test:Config{
//     groups: ["byte"]
// }
// public isolated function testRecordsWithBytes() returns error? {
//     string schema = string `
//         {
//             "namespace": "example.avro",
//             "type": "record",
//             "name": "Student",
//             "fields": [
//                 {"name": "name", "type": "string"},
//                 {"name": "bytes", "type": {"type": "array", "items": "int"}}
//             ]
//         }`;

//     ByteRecord student = {
//         name: "Liam",
//         bytes: "test".toAvro()
//     };

//     Avro avro = check new(schema);
//     byte[] serialize = check avro.toAvro(student);
//     ByteRecord deserialize = check avro.fromAvro(serialize);
//     test:assertEquals(student, deserialize);
// }

// @test:Config{
//     groups: ["byte"]
// }
// public isolated function testRecordsWithByteString() returns error? {
//     string schema = string `
//         {
//             "type": "record",
//             "name": "ExampleRecord",
//             "fields": [
//                 {"name": "name", "type": "string"},
//                 {"name": "bytez", "type": "bytes"}
//             ]
//         }`;

//     Student7 student = {
//         name: "Liam",
//         bytez: "QQ=="
//     };

//     Avro avro = check new(schema);
//     byte[] serialize = check avro.toAvro(student);
//     byte[] dd = [24, 72, 101, 108, 108, 111, 32, 119, 111, 114, 108, 100, 51, 2, 65];
//     Student7 deserialize = check avro.fromAvro(dd);
//     test:assertEquals(student, deserialize);
// }

// public type Student7 record {
//     string name;
//     string bytez;
// };

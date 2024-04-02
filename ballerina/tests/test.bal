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

import ballerina/io;
import ballerina/test;

@test:Config {}
public isolated function testEnums() returns error? {
    string schema = string `
        {
            "type" : "enum",
            "name" : "Numbers", 
            "namespace": "data", 
            "symbols" : [ "ONE", "TWO", "THREE", "FOUR" ]
        }`;

    Numbers number = "ONE";

    Avro avro = check new (schema);
    byte[] encode = check avro.toAvro(number);
    Numbers deserialize = check avro.fromAvro(encode);
    test:assertEquals(number, deserialize);
}

@test:Config {
    groups: ["errors"]
}
public isolated function testEnumsWithString() returns error? {
    string schema = string `
        {
            "type" : "enum",
            "name" : "Numbers", 
            "namespace": "data", 
            "symbols" : [ "ONE", "TWO", "THREE", "FOUR" ]
        }`;

    string number = "FIVE";

    Avro avro = check new (schema);
    byte[]|error encode = avro.toAvro(number);
    test:assertTrue(encode is error);
}

@test:Config {}
public isolated function testMaps() returns error? {
    string schema = string `
        {
            "type": "map",
            "values" : "int",
            "default": {}
        }`;

    map<int> colors = {"red": 0, "green": 1, "blue": 2};

    Avro avro = check new (schema);
    byte[] encode = check avro.toAvro(colors);
    map<int> deserialize = check avro.fromAvro(encode);
    test:assertEquals(colors, deserialize);
}

@test:Config {}
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

    Avro avro = check new (schema);
    byte[] serialize = check avro.toAvro(lecturer);
    Lecturer deserialize = check avro.fromAvro(serialize);
    test:assertEquals(lecturer, deserialize);
}

@test:Config {}
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
        colors: ["maroon", "dark red", "light red"]
    };

    Avro avro = check new (schema);
    byte[] serialize = check avro.toAvro(colors);
    Color deserialize = check avro.fromAvro(serialize);
    test:assertEquals(colors, deserialize);
}

@test:Config {
    groups: ["errors", "qwe"]
}
public isolated function testArraysInRecordsWithInvalidSchema() returns error? {
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
        colors: ["maroon", "dark red", "light red"]
    };

    Avro avroProducer = check new (schema);
    byte[] serialize = check avroProducer.toAvro(colors);

    string schema2 = string `
    {
        "namespace": "example.avro",
        "type": "record",
        "name": "Student",
        "fields": [
            {"name": "name", "type": "string"},
            {"name": "colors", "type": {"type": "array", "items": "int"}}
        ]
    }`;
    Avro avroConsumer = check new (schema2);
    Color|Error deserialize = avroConsumer.fromAvro(serialize);
    test:assertTrue(deserialize is error);
}

@test:Config {}
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

    Avro avro = check new (schema);
    byte[] serialize = check avro.toAvro(student);
    Student deserialize = check avro.fromAvro(serialize);
    test:assertEquals(student, deserialize);
}

@test:Config {}
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

    Avro avro = check new (schema);
    byte[] encode = check avro.toAvro(student);
    Person deserialize = check avro.fromAvro(encode);
    test:assertEquals(student, deserialize);
}

@test:Config {}
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

    Avro avro = check new (schema);
    byte[] serialize = check avro.toAvro(course);
    Course deserialize = check avro.fromAvro(serialize);
    test:assertEquals(course, deserialize);
}

@test:Config {}
public isolated function testArrays() returns error? {
    string schema = string `
        {
            "type": "array",
            "name" : "StringArray", 
            "namespace": "data", 
            "items": "string"
        }`;

    string[] colors = ["red", "green", "blue"];

    Avro avro = check new (schema);
    byte[] encode = check avro.toAvro(colors);
    string[] deserializeJson = check avro.fromAvro(encode);
    test:assertEquals(deserializeJson, colors);
}

@test:Config {}
public isolated function testIntValue() returns error? {

    string schema = string `
        {
            "type": "int",
            "name" : "intValue", 
            "namespace": "data"
        }`;

    int value = 5;

    Avro avro = check new (schema);
    byte[] encode = check avro.toAvro(value);
    int deserializeJson = check avro.fromAvro(encode);
    test:assertEquals(deserializeJson, value);
}

@test:Config {}
public isolated function testFloatValue() returns error? {

    string schema = string `
        {
            "type": "float",
            "name" : "floatValue", 
            "namespace": "data"
        }`;

    float value = 5.5;

    Avro avro = check new (schema);
    byte[] encode = check avro.toAvro(value);
    float deserializeJson = check avro.fromAvro(encode);
    test:assertEquals(deserializeJson, value);
}

@test:Config {}
public isolated function testDoubleValue() returns error? {

    string schema = string `
        {
            "type": "double",
            "name" : "doubleValue", 
            "namespace": "data"
        }`;

    float value = 5.5595;

    Avro avro = check new (schema);
    byte[] encode = check avro.toAvro(value);
    float deserializeJson = check avro.fromAvro(encode);
    test:assertEquals(deserializeJson, value);
}

@test:Config {}
public isolated function testLongValue() returns error? {

    string schema = string `
        {
            "type": "long",
            "name" : "longValue", 
            "namespace": "data"
        }`;

    int value = 555950000000000000;

    Avro avro = check new (schema);
    byte[] encode = check avro.toAvro(value);
    int deserializeJson = check avro.fromAvro(encode);
    test:assertEquals(deserializeJson, value);
}

@test:Config {
    groups: ["primitive"]
}
public isolated function testStringValue() returns error? {

    string schema = string `
        {
            "type": "string",
            "name" : "stringValue", 
            "namespace": "data"
        }`;

    string value = "test";

    Avro avro = check new (schema);
    byte[] encode = check avro.toAvro(value);
    string deserializeJson = check avro.fromAvro(encode);
    test:assertEquals(deserializeJson, value);
}

@test:Config {}
public isolated function testBoolean() returns error? {

    string schema = string `
        {
            "type": "boolean",
            "name" : "booleanValue", 
            "namespace": "data"
        }`;

    boolean value = true;

    Avro avro = check new (schema);
    byte[] encode = check avro.toAvro(value);
    boolean deserializeJson = check avro.fromAvro(encode);
    test:assertEquals(deserializeJson, value);
}

@test:Config {}
public isolated function testNullValues() returns error? {

    string schema = string `
        {
            "type": "null",
            "name" : "nullValue", 
            "namespace": "data"
        }`;

    Avro avro = check new (schema);
    byte[] encode = check avro.toAvro(());
    () deserializeJson = check avro.fromAvro(encode);
    test:assertEquals(deserializeJson, ());
}

@test:Config {}
public isolated function testNullValuesWithNonNullData() returns error? {

    string schema = string `
        {
            "type": "null",
            "name" : "nullValue", 
            "namespace": "data"
        }`;

    Avro avro = check new (schema);
    byte[]|error encode = avro.toAvro("string");
    test:assertTrue(encode is error);
}

@test:Config {}
public isolated function testFixed() returns error? {

    string schema = string `
        {
            "type": "fixed",
            "name": "name",
            "size": 16
        }`;

    byte[] value = "u00ffffffffffffx".toBytes();

    Avro avro = check new (schema);
    byte[] encode = check avro.toAvro(value);
    byte[] deserialize = check avro.fromAvro(encode);
    test:assertEquals(deserialize, value);
}

// @test:Config{
//     groups: ["byte2"]
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

//     BytesRec student = {
//         name: "Liam",
//         bytez: "data".toBytes()
//     };

//     Avro avro = check new(schema);
//     byte[] serialize = check avro.toAvro(student);
//     BytesRec deserialize = check avro.fromAvro(serialize);
//     test:assertEquals(student, deserialize);
// }

@test:Config {}
public function testDbSchemaWithRecords() returns error? {
    string schema = string `
        {
            "connect.name": "io.debezium.connector.sqlserver.SchemaChangeKey",
            "connect.version": 1,
            "fields": [
                {
                "name": "databaseName",
                "type": "string"
                }
            ],
            "name": "SchemaChangeKey",
            "namespace": "io.debezium.connector.sqlserver",
            "type": "record"
        }`;

    SchemaChangeKey changeKey = {
        databaseName: "my-db"
    };

    Avro avro = check new (schema);
    byte[] serialize = check avro.toAvro(changeKey);
    SchemaChangeKey deserialize = check avro.fromAvro(serialize);
    test:assertEquals(changeKey, deserialize);

}

@test:Config {}
public function testComplexDbSchema() returns error? {
    string jsonFileName = string `tests/resources/schema_1.json`;
    json result = check io:fileReadJson(jsonFileName);
    string schema = result.toString();

    Envelope envelope = {
        before: {
            ID: 1,
            OfferID: "offer1",
            PropertyId: 100,
            PlayerUnityID: "player1",
            HALoOfferStatus: "status1",
            StatusDateTime: 1633020142,
            OfferSegmentID: 200,
            RedemptionDate: 1633020142,
            OfferItemID: 300,
            OfferPrizeCode: "prize1",
            AmountRedeemed: 500.0,
            ItemQuantity: 5,
            OfferType: "type1",
            CreatedDate: 1633020142,
            CreatedBy: "creator1",
            UpdatedDate: 1633020142,
            UpdatedBy: "updater1"
        },
        after: {
            ID: 2,
            OfferID: "offer2",
            PropertyId: 101,
            PlayerUnityID: "player2",
            HALoOfferStatus: "status2",
            StatusDateTime: 1633020143,
            OfferSegmentID: 201,
            RedemptionDate: 1633020143,
            OfferItemID: 301,
            OfferPrizeCode: "prize2",
            AmountRedeemed: 600.0,
            ItemQuantity: 6,
            OfferType: "type2",
            CreatedDate: 1633020143,
            CreatedBy: "creator2",
            UpdatedDate: 1633020143,
            UpdatedBy: "updater2"
        },
        'source: {
            version: "1.0",
            connector: "connector1",
            name: "source1",
            ts_ms: 1633020144,
            snapshot: "snapshot1",
            db: "db1",
            sequence: "sequence1",
            schema: "schema1",
            'table: "table1",
            change_lsn: "lsn1",
            commit_lsn: "lsn2",
            event_serial_no: 1
        },
        op: "op1",
        ts_ms: 1633020145,
        'transaction: {
            id: "transaction1",
            total_order: 1,
            data_collection_order: 1
        }
    };

    Avro avro = check new (schema);
    byte[] serialize = check avro.toAvro(envelope);
    Envelope deserialize = check avro.fromAvro(serialize);
    test:assertEquals(envelope, deserialize);
}

@test:Config {
}
public function testComplexDbSchemaWithNestedRecords() returns error? {
    string jsonFileName = string `tests/resources/schema_2.json`;
    json result = check io:fileReadJson(jsonFileName);
    string schema = result.toString();

    Envelope2 envelope2 = {
        before: {
            CorePlayerID: 123,
            AccountNumber: "123456",
            LastName: "Doe",
            FirstName: "John",
            MiddleName: "M",
            Gender: "M",
            Language: "English",
            Discreet: false,
            Deceased: false,
            IsBanned: false,
            EmailAddress: "john.doe@example.com",
            IsVerified: true,
            EmailStatus: "Verified",
            MobilePhone: "1234567890",
            HomePhone: "0987654321",
            HomeStreetAddress: "123 Street",
            HomeCity: "City",
            HomeState: "State",
            HomePostalCode: "12345",
            HomeCountry: "Country",
            AltStreetAddress: "456 Street",
            AltCity: "Alt City",
            AltState: "Alt State",
            AltCountry: "Alt Country",
            DateOfBirth: 1234567890,
            EnrollDate: 1234567890,
            PredomPropertyId: "PropertyId",
            AccountType: "Type",
            InsertDtm: 1234567890,
            AltPostalCode: "54321",
            BatchID: 123,
            GlobalRank: "Rank",
            GlobalValuationScore: 1.0,
            PlayerType: "Type",
            AccountStatus: "Status",
            RegistrationSource: "Source",
            BannedReason: "Reason",
            TierCode: "Code",
            TierName: "Name",
            TierEndDate: 1234567890,
            VIPFlag: false
        },
        after: {
            CorePlayerID: 456,
            AccountNumber: "654321",
            LastName: "Smith",
            FirstName: "Jane",
            MiddleName: "K",
            Gender: "F",
            Language: "Spanish",
            Discreet: true,
            Deceased: false,
            IsBanned: false,
            EmailAddress: "jane.smith@example.com",
            IsVerified: false,
            EmailStatus: "Unverified",
            MobilePhone: "0987654321",
            HomePhone: "1234567890",
            HomeStreetAddress: "456 Street",
            HomeCity: "Alt City",
            HomeState: "Alt State",
            HomePostalCode: "54321",
            HomeCountry: "Alt Country",
            AltStreetAddress: "123 Street",
            AltCity: "City",
            AltState: "State",
            AltCountry: "Country",
            DateOfBirth: 9876543210,
            EnrollDate: 9876543210,
            PredomPropertyId: "AltPropertyId",
            AccountType: "AltType",
            InsertDtm: 9876543210,
            AltPostalCode: "12345",
            BatchID: 456,
            GlobalRank: "AltRank",
            GlobalValuationScore: 2.0,
            PlayerType: "AltType",
            AccountStatus: "AltStatus",
            RegistrationSource: "AltSource",
            BannedReason: "AltReason",
            TierCode: "AltCode",
            TierName: "AltName",
            TierEndDate: 9876543210,
            VIPFlag: true
        },
        'source: {
            version: "1.0",
            connector: "connector",
            name: "name",
            ts_ms: 123456789,
            snapshot: "snapshot",
            db: "db",
            sequence: "sequence",
            schema: "schema",
            'table: "table",
            change_lsn: "lsn",
            commit_lsn: "lsn",
            event_serial_no: 1
        },
        op: "op",
        ts_ms: 123456789,
        'transaction: {
            id: "id",
            total_order: 1,
            data_collection_order: 1
        },
        MessageSource: "MessageSource"
    };

    Avro avro = check new (schema);
    byte[] serialize = check avro.toAvro(envelope2);
    Envelope2 deserialize = check avro.fromAvro(serialize);
    test:assertEquals(envelope2, deserialize);
}


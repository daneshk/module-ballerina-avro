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

import ballerina/jballerina.java;

public class Avro {
    private handle avroSchema;
    private typedesc<anydata> dataType;

    # Generates a schema for a given data type.
    #
    # avro:Avro avro = check new(string `{"type": "int", "name" : "intValue", "namespace": "data" }`);
    #
    # + schema - The Avro schema
    # + return - A `avro:Error` if the schema is not valid or else `()`
    public isolated function init(string schema) returns Error? {
        self.avroSchema = generateSchema(schema);
    }

    # Serializes the given data according to the Avro format.
    #
    # avro:Avro avro = check new(string `{"type": "int", "name" : "intValue", "namespace": "data" }`); \
    # int value = 5; \
    # byte[] serializeData = check avro.toAvro(value);
    # 
    # + data - The data to be serialized
    # + return - A `byte` array of the serialized data or else an `avro:Error`
    public isolated function toAvro(anydata data) returns byte[]|Error = @java:Method {
        'class: "io.ballerina.lib.avro.Avro"
    }  external;

    # Deserializes the given Avro encoded message to the given data type.
    # 
    # byte[] data = // Avro encoded message ; \
    # int deserializeData = check avro.fromAvro(data);
    # 
    # + encodedMessage - The Avro encoded message
    # + T - Default parameter use to infer the user specified type
    # + return - A deserialized data with the given type or else an `avro:Error`
    public isolated function fromAvro(byte[] encodedMessage, typedesc<anydata> T = <>)
        returns T|Error = @java:Method {
        'class: "io.ballerina.lib.avro.Avro"
    }  external;
}

isolated function generateSchema(string schema) returns handle = @java:Constructor {
    'class: "io.ballerina.lib.avro.Avro"
} external;

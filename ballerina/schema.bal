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

# The avro schema implementation to support Avro serialization and deserialization.
public class Schema {

    # Initializes the Avro schema with the given schema definition.
    #
    # ```ballerina
    # avro:Schema schema = check new(string `{"type": "int", "name" : "intValue", "namespace": "data" }`);
    # ```
    #
    # + schema - The Avro schema definition as a string
    # + return - An `avro:Error` if the schema is not valid, otherwise nil
    public isolated function init(string schema) returns Error? {
        self.generateSchema(schema);
    }

    isolated function generateSchema(string schema) = @java:Method {
        'class: "io.ballerina.lib.avro.Avro"
    } external;

    # Serializes the given data according to the Avro format.
    #
    # ```ballerina
    # avro:Schema schema = check new(string `{"type": "int", "name" : "data", "namespace": "example.avro" }`);
    # int value = 5;
    # byte[] serializedData = check schema.toAvro(value);
    # ```
    #
    # + data - The data to be serialized
    # + return - A `byte` array of the serialized data or else an `avro:Error`
    public isolated function toAvro(anydata data) returns byte[]|Error = @java:Method {
        'class: "io.ballerina.lib.avro.Avro"
    } external;

    # Deserializes the given Avro encoded message to the given data type.
    #
    # ```ballerina
    # avro:Schema schema = check new(string `{"type": "int", "name" : "data", "namespace": "example.avro" }`);
    # byte[] data = [10] //Avro encoded message;
    # int deserializedData = check schema.fromAvro(data);
    # ```
    #
    # + data - The Avro serialized data to be deserialized
    # + targetType - The type to be deserialized, inferred from the return type
    # + return - A deserialized data for the given type or else an `avro:Error`
    public isolated function fromAvro(byte[] data, typedesc<anydata> targetType = <>)
        returns targetType|Error = @java:Method {
        'class: "io.ballerina.lib.avro.Avro"
    } external;
}

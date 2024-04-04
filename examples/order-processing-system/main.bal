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

import ballerina/avro;
import ballerina/io;

public type OrderRequest record {
    string orderId;
    string customerId;
    string productId;
    int quantity;
    float price;
    string status;
};

public function main() returns error? {
    string schema = string `{
        "type": "record",
        "name": "OrderRequest",
        "fields": [
            {"name": "orderId", "type": "string"},
            {"name": "customerId", "type": "string"},
            {"name": "productId", "type": "string"},
            {"name": "quantity", "type": "int"},
            {"name": "price", "type": "float"},
            {"name": "status", "type": "string"}
        ]
    }`;

    avro:Schema orderRequestSchema = check new (schema);
    OrderRequest orderRequest = {
        orderId: "ORD123",
        customerId: "CUST456",
        productId: "PROD789",
        quantity: 2,
        price: 100.0,
        status: "Pending"
    };

    byte[] serializedOrderRequest = check orderRequestSchema.toAvro(orderRequest);
    io:println("Serialized Order Request: ", serializedOrderRequest);

    OrderRequest deserializedOrderRequest = check orderRequestSchema.fromAvro(serializedOrderRequest);
    io:println("Deserialized Order Request: ", deserializedOrderRequest);
}

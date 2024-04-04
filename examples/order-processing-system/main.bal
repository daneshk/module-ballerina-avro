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

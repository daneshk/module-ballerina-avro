## Overview

Avro is an open-source data serialization system that enables efficient binary serialization and deserialization. It allows users to define schemas for structured data, providing better representation and fast serialization/deserialization. Avro's schema evolution capabilities ensure compatibility and flexibility in evolving data systems.

The Ballerina Avro module provides the capability to efficiently serialize and deserialize data using Avro schemas.

### Client

The Client is will take the Avro schema in string format. And will return an error if the schema is not a valid Avro schema. The client can be used to serialize and deserilize data and the data should be in the correct format.

A `Client` can be defined using the string value of an Avro schema as shown below:

```ballerina
avro:Schema avro = check new(string `{"type": "int", "name" : "intValue", "namespace": "data" }`);
```

### APIs associated with Avro

- **toAvro**: Serializes the given data according to the Avro format.
- **fromAvro**: Deserializes the given Avro encoded message to the given data type.

#### `toAvro()` API

Serializes the given data according to the Avro format.

```ballerina
import ballerina/avro;

public function main() returns error? {
    int value = 5;
    byte[] serializeData = check avro.toAvro(value);
}
```

#### `fromAvro()` API

Deserializes the given Avro encoded message to the given data type.

```ballerina
import ballerina/avro;

public function main() returns error? {
    byte[] data = // Avro encoded message ;
    int deserializeData = check avro.fromAvro(data);
}
```

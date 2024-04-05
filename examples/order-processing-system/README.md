# Avro Serialization/Deserialization with Ballerina: An Order Processing System Example

This example demonstrate on using Avro serialization/deserialization operations in a order processing system. 

Here, a client places an order and the metadata about the order request is serialized according to an Avro schema and sent to the system. The system retrieves the byte data and converts it back into the correct order request type using the same Avro schema. Upon successful execution, the user will see the serialized and deserialized order request in the console.

## Running an Example

Execute the following commands to build an example from the source:

* To build an example:

    ```bash
    bal build
    ```

* To run an example:

    ```bash
    bal run
    ```

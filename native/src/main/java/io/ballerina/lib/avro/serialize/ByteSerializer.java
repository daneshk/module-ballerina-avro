package io.ballerina.lib.avro.serialize;

import io.ballerina.lib.avro.serialize.visitor.SerializeVisitor;
import io.ballerina.runtime.api.values.BArray;

import java.nio.ByteBuffer;

public class ByteSerializer extends Serializer {

    @Override
    public Object convert(SerializeVisitor serializeVisitor, Object data) {
        return ByteBuffer.wrap(((BArray) data).getByteArray());
    }
}

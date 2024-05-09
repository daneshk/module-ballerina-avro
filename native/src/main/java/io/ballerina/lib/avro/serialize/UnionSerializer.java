package io.ballerina.lib.avro.serialize;

import io.ballerina.lib.avro.serialize.visitor.SerializeVisitor;
import org.apache.avro.Schema;

public class UnionSerializer extends Serializer {

    public UnionSerializer(Schema schema) {
        super(schema);
    }

    @Override
    public Object convert(SerializeVisitor serializeVisitor, Object data) throws Exception {
        return serializeVisitor.visitUnion(this, data);
    }
}

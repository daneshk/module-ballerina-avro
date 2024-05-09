package io.ballerina.lib.avro.deserialize;

import io.ballerina.lib.avro.deserialize.visitor.DeserializeVisitor;
import org.apache.avro.generic.GenericData;

public class ByteDeserializer extends Deserializer {
    @Override
    public Object fromAvro(DeserializeVisitor visitor, Object data) throws Exception {
        return visitor.visitBytes(data);
    }

    @Override
    public Object visit(DeserializeVisitor visitor, GenericData.Array<Object> data) throws Exception {
        return visitor.visitBytes(data);
    }
}

package io.ballerina.lib.avro.deserialize;

import io.ballerina.lib.avro.deserialize.visitor.DeserializeVisitor;
import io.ballerina.runtime.api.types.Type;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;

public class UnionDeserializer extends Deserializer {

    public UnionDeserializer(Schema schema, Type type) {
        super(schema, type);
    }

    @Override
    public Object visit(DeserializeVisitor visitor, Object data) throws Exception {
        return visitor.visit(this, (GenericData.Array<Object>) data);
    }

    public Object visit(DeserializeVisitor visitor, GenericData.Array<Object> data) throws Exception {
        return visitor.visit(this, data);
    }
}

package io.ballerina.lib.avro.deserialize;

import io.ballerina.lib.avro.deserialize.visitor.DeserializeVisitor;
import io.ballerina.runtime.api.types.Type;
import org.apache.avro.generic.GenericData;

public class EnumDeserializer extends Deserializer {

    public EnumDeserializer(Type type) {
        super(type);
    }

    @Override
    public Object fromAvro(DeserializeVisitor visitor, Object data) {
        return visitor.visit(this, (GenericData.Array<Object>) data);
    }

    @Override
    public Object visit(DeserializeVisitor visitor, GenericData.Array<Object> data) throws Exception {
        return visitor.visit(this, data);
    }

}

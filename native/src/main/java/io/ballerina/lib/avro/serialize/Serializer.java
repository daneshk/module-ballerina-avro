package io.ballerina.lib.avro.serialize;

import io.ballerina.lib.avro.serialize.visitor.SerializeVisitor;
import io.ballerina.runtime.api.types.Type;
import io.ballerina.runtime.api.utils.TypeUtils;
import org.apache.avro.Schema;

public abstract class Serializer {

    private final String schema;
    private final Type type;

    public Serializer() {
        this.type = null;
        this.schema = null;
    }

    public Serializer(Schema schema) {
        this.type = null;
        this.schema = schema.toString();
    }

    public Serializer(Schema schema, Type type) {
        this.type = TypeUtils.getImpliedType(type);
        this.schema = schema.toString();
    }

    public Schema getSchema() {
        return new Schema.Parser().parse(schema);
    }

    public Type getType() {
        return TypeUtils.getImpliedType(type);
    }

    public abstract Object convert(SerializeVisitor serializeVisitor, Object data) throws Exception;
}

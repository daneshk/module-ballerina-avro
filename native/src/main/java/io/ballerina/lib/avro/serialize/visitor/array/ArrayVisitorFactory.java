package io.ballerina.lib.avro.serialize.visitor.array;

import org.apache.avro.Schema;

public class ArrayVisitorFactory {
    public static IArrayVisitor createVisitor(Schema schema) {
        switch (schema.getElementType().getType()) {
            case NULL:
                return null;
            case ARRAY:
                return new ArrayVisitor();
            case ENUM:
                return new EnumArrayVisitor();
            case UNION:
                return new UnionArrayVisitor();
            case FIXED:
                return new FixedArrayVisitor();
            case RECORD:
                return new RecordArrayVisitor();
            case MAP:
                return new MapArrayVisitor();
            default:
                return new PrimitiveArrayVisitor();
        }
    }
}

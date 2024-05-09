package io.ballerina.lib.avro.serialize.visitor.array;

import io.ballerina.runtime.api.values.BArray;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;

import java.util.Map;

import static java.util.Map.entry;

public class UnionArrayVisitor implements IArrayVisitor {
    @Override
    public GenericData.Array<Object> visit(BArray data, Schema schema, GenericData.Array<Object> array) {
        Map<Schema.Type, IArrayVisitor> visitorMap = Map.ofEntries(
                entry(Schema.Type.ARRAY, new ArrayVisitor()),
                entry(Schema.Type.MAP, new MapArrayVisitor()),
                entry(Schema.Type.RECORD, new RecordArrayVisitor()),
                entry(Schema.Type.FIXED, new FixedArrayVisitor()),
                entry(Schema.Type.BOOLEAN, new PrimitiveArrayVisitor()),
                entry(Schema.Type.STRING, new PrimitiveArrayVisitor()),
                entry(Schema.Type.INT, new PrimitiveArrayVisitor()),
                entry(Schema.Type.LONG, new PrimitiveArrayVisitor()),
                entry(Schema.Type.DOUBLE, new PrimitiveArrayVisitor()),
                entry(Schema.Type.BYTES, new PrimitiveArrayVisitor()),
                entry(Schema.Type.FLOAT, new PrimitiveArrayVisitor())
        );

        Schema elementType = schema.getElementType();
        for (Schema schema1 : elementType.getTypes()) {
            IArrayVisitor visitor = visitorMap.get(schema1.getType());
            if (visitor != null) {
                return visitor.visit(data, schema1, array);
            }
        }
        return null;
    }
}

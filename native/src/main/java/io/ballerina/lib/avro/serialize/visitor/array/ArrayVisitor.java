package io.ballerina.lib.avro.serialize.visitor.array;

import io.ballerina.lib.avro.serialize.ArraySerializer;
import io.ballerina.lib.avro.serialize.visitor.SerializeVisitor;
import io.ballerina.runtime.api.values.BArray;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;

import java.util.Arrays;
import java.util.Objects;

public class ArrayVisitor implements IArrayVisitor {
    public GenericData.Array<Object> visit(BArray data, Schema schema, GenericData.Array<Object> array) {
        Arrays.stream(data.getValues())
                .filter(Objects::nonNull)
                .forEach(value -> {
                    try {
                        array.add(new SerializeVisitor().visit(new ArraySerializer(schema.getElementType()),
                                (BArray) value));
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
        return array;
    }
}

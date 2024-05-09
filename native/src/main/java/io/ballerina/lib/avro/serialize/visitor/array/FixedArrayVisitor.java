package io.ballerina.lib.avro.serialize.visitor.array;

import io.ballerina.runtime.api.values.BArray;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericFixed;

import java.util.Arrays;
import java.util.Objects;

public class FixedArrayVisitor implements IArrayVisitor {
    @Override
    public GenericData.Array<Object> visit(BArray data, Schema schema, GenericData.Array<Object> array) {
        Arrays.stream(data.getValues())
                .filter(Objects::nonNull)
                .forEach(bytes -> {
                    GenericFixed genericFixed = new GenericData.Fixed(schema, ((BArray) bytes).getByteArray());
                    array.add(genericFixed);
                });
        return array;
    }
}

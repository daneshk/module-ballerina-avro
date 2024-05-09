package io.ballerina.lib.avro.serialize.visitor.array;

import io.ballerina.runtime.api.values.BArray;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;

import java.util.Arrays;
import java.util.Objects;

public class EnumArrayVisitor implements IArrayVisitor {
    @Override
    public GenericData.Array<Object> visit(BArray data, Schema schema, GenericData.Array<Object> array) {
        Arrays.stream((data.getValues() == null) ? data.getStringArray() : data.getValues())
                .filter(Objects::nonNull)
                .forEach(value -> {
                    try {
                        array.add(new GenericData.EnumSymbol(schema.getElementType(), value));
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
        return array;
    }
}

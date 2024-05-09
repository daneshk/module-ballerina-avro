package io.ballerina.lib.avro.serialize.visitor.array;

import io.ballerina.lib.avro.serialize.MapSerializer;
import io.ballerina.lib.avro.serialize.visitor.SerializeVisitor;
import io.ballerina.runtime.api.values.BArray;
import io.ballerina.runtime.api.values.BMap;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;

import java.util.Arrays;
import java.util.Objects;

public class MapArrayVisitor implements IArrayVisitor {
    @Override
    public GenericData.Array<Object> visit(BArray data, Schema schema, GenericData.Array<Object> array) {
        Arrays.stream(data.getValues())
                .filter(Objects::nonNull)
                .forEach(record -> {
                    try {
                        array.add(new SerializeVisitor().visit(new MapSerializer(schema.getElementType()),
                                                               (BMap<?, ?>) record));
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
        return array;
    }
}

package io.ballerina.lib.avro.serialize.visitor.array;

import io.ballerina.runtime.api.values.BArray;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;

public class PrimitiveArrayVisitor implements IArrayVisitor {
    @Override
    public GenericData.Array<Object> visit(BArray data, Schema schema, GenericData.Array<Object> array) {
        Schema.Type type = schema.getType().equals(Schema.Type.ARRAY)
                ? schema.getElementType().getType()
                : schema.getType();

        switch (type) {
            case STRING ->
                    array.addAll(Arrays.asList(data.getStringArray()));
            case INT -> {
                for (long obj: data.getIntArray()) {
                    array.add(((Long) obj).intValue());
                }
            }
            case LONG -> {
                for (Object obj: data.getIntArray()) {
                    array.add(obj);
                }
            }
            case FLOAT -> {
                for (Double obj: data.getFloatArray()) {
                    array.add(obj.floatValue());
                }
            }
            case DOUBLE -> {
                for (Object obj: data.getFloatArray()) {
                    array.add(obj);
                }
            }
            case BOOLEAN -> {
                for (Object obj: data.getBooleanArray()) {
                    array.add(obj);
                }
            }
            default -> visitBytes(data, array);
        }
        return array;
    }


    public static GenericData.Array<Object> visitBytes(BArray data, GenericData.Array<Object> array) {
        Arrays.stream(data.getValues())
                .filter(Objects::nonNull)
                .forEach(bytes -> {
                    ByteBuffer byteBuffer = ByteBuffer.allocate(((BArray) bytes).getByteArray().length);
                    byteBuffer.put(((BArray) bytes).getByteArray());
                    byteBuffer.position(0);
                    array.add(byteBuffer);
                });
        return array;
    }
}


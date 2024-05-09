/*
 * Copyright (c) 2024, WSO2 LLC. (http://www.wso2.com)
 *
 * WSO2 LLC. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.ballerina.lib.avro.serialize.visitor;

import io.ballerina.lib.avro.serialize.ArraySerializer;
import io.ballerina.lib.avro.serialize.EnumSerializer;
import io.ballerina.lib.avro.serialize.FixedSerializer;
import io.ballerina.lib.avro.serialize.MapSerializer;
import io.ballerina.lib.avro.serialize.PrimitiveDeserializer;
import io.ballerina.lib.avro.serialize.RecordSerializer;
import io.ballerina.lib.avro.serialize.Serializer;
import io.ballerina.lib.avro.serialize.UnionSerializer;
import io.ballerina.lib.avro.serialize.visitor.array.ArrayVisitorFactory;
import io.ballerina.lib.avro.serialize.visitor.array.IArrayVisitor;
import io.ballerina.runtime.api.TypeTags;
import io.ballerina.runtime.api.types.Type;
import io.ballerina.runtime.api.utils.StringUtils;
import io.ballerina.runtime.api.utils.TypeUtils;
import io.ballerina.runtime.api.values.BArray;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BString;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class SerializeVisitor implements ISerializeVisitor {

    public Serializer createSerializer(Schema schema) {
        return switch (schema.getValueType().getType()) {
            case INT, LONG, FLOAT, DOUBLE, BOOLEAN, STRING, BYTES ->
                    new PrimitiveDeserializer(schema.getValueType());
            case RECORD ->
                    new RecordSerializer(schema.getValueType());
            case MAP ->
                    new MapSerializer(schema.getValueType());
            case ARRAY ->
                    new ArraySerializer(schema.getValueType());
            case ENUM ->
                    new EnumSerializer(schema.getValueType());
            case FIXED ->
                    new FixedSerializer(schema.getValueType());
            default ->
                    throw new IllegalArgumentException("Unsupported schema type: " + schema.getValueType().getType());
        };
    }

    @Override
    public String visitString(Object data) {
        return ((BString) data).getValue();
    }

    @Override
    public GenericRecord visit(RecordSerializer recordSerializer, BMap<?, ?> data) throws Exception {
        GenericRecord genericRecord = new GenericData.Record(recordSerializer.getSchema());
        for (Schema.Field field : recordSerializer.getSchema().getFields()) {
            Object fieldData = data.get(StringUtils.fromString(field.name()));
            genericRecord.put(field.name(), serializeField(field.schema(), fieldData));
        }
        return genericRecord;
    }

    private Object serializeField(Schema schema, Object fieldData) throws Exception {
        Schema.Type type = schema.getType();
        return switch (type) {
            case RECORD ->
                    new RecordSerializer(schema).convert(this, fieldData);
            case MAP ->
                    new MapSerializer(schema).convert(this, fieldData);
            case ARRAY ->
                    new ArraySerializer(schema).convert(this, fieldData);
            case ENUM ->
                    new EnumSerializer(schema).convert(this, fieldData);
            case UNION ->
                    new UnionSerializer(schema).convert(this, fieldData);
            default ->
                    new PrimitiveDeserializer(schema).convert(this, fieldData);
        };
    }

    @Override
    public Object visit(PrimitiveDeserializer primitiveDeserializer, Object data) {
        switch (primitiveDeserializer.getSchema().getType()) {
            case INT -> {
                return ((Long) data).intValue();
            }
            case FLOAT -> {
                return ((Double) data).floatValue();
            }
            case BYTES -> {
                ByteBuffer byteBuffer = ByteBuffer.allocate(((BArray) data).getByteArray().length);
                byteBuffer.put(((BArray) data).getByteArray());
                byteBuffer.position(0);
                return byteBuffer;
            }
            case STRING -> {
                return data.toString();
            }
            default -> {
                return data;
            }
        }
    }

    public Map<String, Object> visit(MapSerializer mapSerializer, BMap<?, ?> data) throws Exception {
        Map<String, Object> avroMap = new HashMap<>();
        Schema schema = mapSerializer.getSchema();
        if (schema.getType().equals(Schema.Type.UNION)) {
            for (Schema fieldSchema: schema.getTypes()) {
                if (fieldSchema.getType().equals(Schema.Type.MAP)) {
                    schema = fieldSchema;
                }
            }
        }
        for (Object value : data.getKeys()) {
            Serializer serializer = createSerializer(schema);
            avroMap.put(value.toString(), serializer.convert(this, data.get(value)));
        }
        return avroMap;
    }

    @Override
    public Object visit(EnumSerializer enumSerializer, Object data) {
        return new GenericData.EnumSymbol(enumSerializer.getSchema(), data);
    }

    @Override
    public GenericData.Fixed visit(FixedSerializer fixedSerializer, Object data) {
        return new GenericData.Fixed(fixedSerializer.getSchema(), ((BArray) data).getByteArray());
    }

    public GenericData.Array<Object> visit(ArraySerializer arraySerializer, BArray data) {
        GenericData.Array<Object> array = new GenericData.Array<>(data.size(), arraySerializer.getSchema());
        IArrayVisitor visitor = ArrayVisitorFactory.createVisitor(arraySerializer.getSchema());
        return Objects.requireNonNull(visitor).visit(data, arraySerializer.getSchema(), array);
    }

    public Object visitUnion(UnionSerializer unionSerializer, Object data) throws Exception {
        Schema fieldSchema = unionSerializer.getSchema();
        Type typeName = TypeUtils.getType(data);
        switch (typeName.getTag()) {
            case TypeTags.STRING_TAG -> {
                return visitiUnionStrings(data, fieldSchema);
            }
            case TypeTags.ARRAY_TAG -> {
                return visitUnionArrays(data, fieldSchema);
            }
            case TypeTags.MAP_TAG -> {
                return new MapSerializer(fieldSchema).convert(this, data);
            }
            case TypeTags.RECORD_TYPE_TAG -> {
                Schema schema = getRecordSchema(Schema.Type.RECORD, fieldSchema.getTypes());
                return new RecordSerializer(schema).convert(this, data);
            }
            case TypeTags.INT_TAG -> {
                return visitUnionIntegers(data, fieldSchema);
            }
            case TypeTags.FLOAT_TAG -> {
                return visitUnionFloats(data, fieldSchema);
            }
            default -> {
                return data;
            }
        }
    }

    private Object visitUnionFloats(Object data, Schema fieldSchema) {
        return fieldSchema.getTypes().stream()
                .filter(schema -> schema.getType().equals(Schema.Type.FLOAT))
                .findFirst()
                .map(schema -> new PrimitiveDeserializer(schema).convert(this, data))
                .orElse(data);
    }

    private Object visitUnionIntegers(Object data, Schema fieldSchema) {
        return fieldSchema.getTypes().stream()
                .filter(schema -> schema.getType().equals(Schema.Type.INT))
                .findFirst()
                .map(schema -> new PrimitiveDeserializer(schema).convert(this, data))
                .orElse(data);
    }

    private Object visitiUnionStrings(Object data, Schema fieldSchema) {
        return fieldSchema.getTypes().stream()
                .filter(type -> type.getType().equals(Schema.Type.ENUM))
                .findFirst()
                .map(type -> visit(new EnumSerializer(type), data))
                .orElse(visit(new PrimitiveDeserializer(fieldSchema), data.toString()));
    }

    private Object visitUnionArrays(Object data, Schema fieldSchema) {
        for (Schema schema : fieldSchema.getTypes()) {
            switch (schema.getType()) {
                case BYTES -> {
                    return new PrimitiveDeserializer(schema).convert(this, data);
                }
                case FIXED -> {
                    return new FixedSerializer(schema).convert(this, data);
                }
                case ARRAY -> {
                    return new ArraySerializer(schema).convert(this, data);
                }
            }
        }
        return new ArraySerializer(fieldSchema).convert(this, data);
    }

    public static Schema getRecordSchema(Schema.Type givenType, List<Schema> schemas) {
        for (Schema schema: schemas) {
            if (schema.getType().equals(Schema.Type.UNION)) {
                getRecordSchema(givenType, schema.getTypes());
            } else if (schema.getType().equals(givenType)) {
                return schema;
            }
        }
        return null;
    }
}

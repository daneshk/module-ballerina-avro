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

package io.ballerina.lib.avro.deserialize.visitor;

import io.ballerina.lib.avro.deserialize.ArrayDeserializer;
import io.ballerina.lib.avro.deserialize.Deserializer;
import io.ballerina.lib.avro.deserialize.EnumDeserializer;
import io.ballerina.lib.avro.deserialize.FixedDeserializer;
import io.ballerina.lib.avro.deserialize.MapDeserializer;
import io.ballerina.lib.avro.deserialize.PrimitiveDeserializer;
import io.ballerina.lib.avro.deserialize.RecordDeserializer;
import io.ballerina.lib.avro.deserialize.UnionDeserializer;
import io.ballerina.runtime.api.TypeTags;
import io.ballerina.runtime.api.creators.ValueCreator;
import io.ballerina.runtime.api.types.ArrayType;
import io.ballerina.runtime.api.types.Field;
import io.ballerina.runtime.api.types.IntersectionType;
import io.ballerina.runtime.api.types.MapType;
import io.ballerina.runtime.api.types.RecordType;
import io.ballerina.runtime.api.types.ReferenceType;
import io.ballerina.runtime.api.types.Type;
import io.ballerina.runtime.api.utils.StringUtils;
import io.ballerina.runtime.api.utils.TypeUtils;
import io.ballerina.runtime.api.utils.ValueUtils;
import io.ballerina.runtime.api.values.BArray;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BString;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static io.ballerina.lib.avro.Utils.getMutableType;
import static io.ballerina.lib.avro.deserialize.visitor.RecordUtils.processArrayField;
import static io.ballerina.lib.avro.deserialize.visitor.RecordUtils.processBytesField;
import static io.ballerina.lib.avro.deserialize.visitor.RecordUtils.processMapField;
import static io.ballerina.lib.avro.deserialize.visitor.RecordUtils.processRecordField;
import static io.ballerina.lib.avro.deserialize.visitor.RecordUtils.processStringField;
import static io.ballerina.lib.avro.deserialize.visitor.RecordUtils.processUnionField;
import static io.ballerina.runtime.api.utils.StringUtils.fromString;

public class DeserializeVisitor implements IDeserializeVisitor {

    public static Deserializer createDeserializer(Schema schema, Type type) {
        return switch (schema.getElementType().getType()) {
            case UNION ->
                    new UnionDeserializer(schema, type);
            case ARRAY ->
                    new ArrayDeserializer(schema, type);
            case ENUM ->
                    new EnumDeserializer(type);
            case RECORD ->
                    new RecordDeserializer(schema, type);
            case FIXED ->
                    new FixedDeserializer(schema, type);
            default ->
                    new PrimitiveDeserializer(schema, type);
        };
    }

    public BMap<BString, Object> visit(RecordDeserializer recordDeserializer, GenericRecord rec) throws Exception {
        Type originalType = recordDeserializer.getType();
        Type type = recordDeserializer.getType();
        Schema schema = recordDeserializer.getSchema();
        BMap<BString, Object> avroRecord = createAvroRecord(type);
        for (Schema.Field field : schema.getFields()) {
            Object fieldData = rec.get(field.name());
            switch (field.schema().getType()) {
                case MAP ->
                        processMapField(avroRecord, field, fieldData);
                case ARRAY ->
                        processArrayField(avroRecord, field, fieldData);
                case BYTES ->
                        processBytesField(avroRecord, field, fieldData);
                case RECORD ->
                        processRecordField(avroRecord, field, fieldData);
                case STRING ->
                        processStringField(avroRecord, field, fieldData);
                case INT ->
                        avroRecord.put(fromString(field.name()), Long.parseLong(fieldData.toString()));
                case FLOAT ->
                        avroRecord.put(fromString(field.name()), Double.parseDouble(fieldData.toString()));
                case UNION ->
                        processUnionField(type, avroRecord, field, fieldData);
                default ->
                        avroRecord.put(fromString(field.name()), fieldData);
            }
        }

        if (originalType.isReadOnly()) {
            avroRecord.freezeDirect();
        }
        return avroRecord;
    }

    public BMap<BString, Object> visit(MapDeserializer mapDeserializer, Map<String, Object> data) throws Exception {
        BMap<BString, Object> avroRecord = ValueCreator.createMapValue();
        Object[] keys = data.keySet().toArray();
        Schema schema = mapDeserializer.getSchema();
        Type type = mapDeserializer.getType();
        for (Object key : keys) {
            Object value = data.get(key);
            Schema.Type valueType = schema.getValueType().getType();
            switch (valueType) {
                case ARRAY ->
                        processMapArray(avroRecord, schema, (MapType) type, key, (GenericData.Array<Object>) value);
                case BYTES ->
                        avroRecord.put(StringUtils.fromString(key.toString()),
                                       ValueCreator.createArrayValue(((ByteBuffer) value).array()));
                case FIXED ->
                        avroRecord.put(StringUtils.fromString(key.toString()),
                                       ValueCreator.createArrayValue(((GenericFixed) value).bytes()));
                case ENUM, STRING ->
                        avroRecord.put(StringUtils.fromString(key.toString()),
                                       StringUtils.fromString(value.toString()));
                case RECORD ->
                        processMapRecord(avroRecord, schema, (MapType) type, key, (GenericRecord) value);
                case FLOAT ->
                        avroRecord.put(StringUtils.fromString(key.toString()),
                                       Double.parseDouble(value.toString()));
                case MAP ->
                        processMaps(avroRecord, schema, (MapType) type, key, (Map<String, Object>) value);
                default ->
                        avroRecord.put(StringUtils.fromString(key.toString()), value);
            }
        }
        return (BMap<BString, Object>) ValueUtils.convert(avroRecord, type);
    }

    public Object visit(PrimitiveDeserializer primitiveDeserializer, Object data) {
        Schema schema = primitiveDeserializer.getSchema();
        if (schema.getType().equals(Schema.Type.ARRAY)) {
            GenericData.Array<Object> array = (GenericData.Array<Object>) data;
            switch (schema.getElementType().getType()) {
                case STRING -> {
                    return visitStringArray(array);
                }
                case INT -> {
                    return visitIntArray(array);
                }
                case LONG -> {
                    return visitLongArray(array);
                }
                case FLOAT, DOUBLE -> {
                    return visitDoubleArray(array);
                }
                case BOOLEAN -> {
                    return visitBooleanArray(array);
                }
                default -> {
                    return visitBytesArray(array, primitiveDeserializer.getType());
                }
            }
        } else {
            return data;
        }
    }

    public BArray visit(UnionDeserializer unionDeserializer, GenericData.Array<Object> data) throws Exception {
        Type type = unionDeserializer.getType();
        Schema schema = unionDeserializer.getSchema();
        switch (((ArrayType) type).getElementType().getTag()) {
            case TypeTags.STRING_TAG -> {
                return visitStringArray(data);
            }
            case TypeTags.FLOAT_TAG -> {
                return visitDoubleArray(data);
            }
            case TypeTags.BOOLEAN_TAG -> {
                return visitBooleanArray(data);
            }
            case TypeTags.INT_TAG -> {
                return visitIntegerArray(data, schema);
            }
            case TypeTags.RECORD_TYPE_TAG -> {
                return visitRecordArray(data, type, schema);
            }
            case TypeTags.ARRAY_TAG -> {
                return visitUnionArray(data, (ArrayType) type, schema);
            }
            default -> {
                return visitBytes(data);
            }
        }
    }

    private BArray visitRecordArray(GenericData.Array<Object> data, Type type, Schema schema) throws Exception {
        RecordDeserializer recordDeserializer = new RecordDeserializer(schema.getElementType(), type);
        return (BArray) recordDeserializer.visit(this, data);
    }

    private BArray visitUnionArray(GenericData.Array<Object> data, ArrayType type, Schema schema) throws Exception {
        Object[] objects = new Object[data.size()];
        Type elementType = type.getElementType();
        ArrayDeserializer arrayDeserializer = new ArrayDeserializer(schema.getElementType(), elementType);
        int index = 0;
        for (Object currentData : data) {
            Object deserializedObject = arrayDeserializer.visit(this, (GenericData.Array<Object>) currentData);
            objects[index++] = deserializedObject;
        }
        return ValueCreator.createArrayValue(objects, type);
    }

    public BArray visit(RecordDeserializer recordDeserializer, GenericData.Array<Object> data) throws Exception {
        List<Object> recordList = new ArrayList<>();
        Type type = recordDeserializer.getType();
        Schema schema = recordDeserializer.getSchema();
        switch (type.getTag()) {
            case TypeTags.ARRAY_TAG -> {
                for (Object datum : data) {
                    Type fieldType = ((ArrayType) type).getElementType().getCachedReferredType();
                    RecordDeserializer recordDes = new RecordDeserializer(schema.getElementType(), fieldType);
                    recordList.add(recordDes.visit(this, (GenericRecord) datum));
                }
            }
            case TypeTags.TYPE_REFERENCED_TYPE_TAG -> {
                for (Object datum : data) {
                    Type fieldType = ((ReferenceType) type).getReferredType();
                    RecordDeserializer recordDes = new RecordDeserializer(schema.getElementType(), fieldType);
                    recordList.add(recordDes.visit(this, (GenericRecord) datum));
                }
            }
        }
        return ValueCreator.createArrayValue(recordList.toArray(new Object[data.size()]), (ArrayType) type);
    }

    private BMap<BString, Object> createAvroRecord(Type type) {
        return ValueCreator.createRecordValue((RecordType) getMutableType(type));
    }

    private void processMaps(BMap<BString, Object> avroRecord, Schema schema,
                             MapType type, Object key, Map<String, Object> value) throws Exception {
        Schema fieldSchema = schema.getValueType();
        Type fieldType = type.getConstrainedType();
        MapDeserializer mapDes = new MapDeserializer(fieldSchema, fieldType);
        Object fieldValue = mapDes.visit(this, value);
        avroRecord.put(fromString(key.toString()), fieldValue);
    }

    private void processMapRecord(BMap<BString, Object> avroRecord, Schema schema,
                                  MapType type, Object key, GenericRecord value) throws Exception {
        Type fieldType = type.getConstrainedType().getCachedReferredType();
        RecordDeserializer recordDes = new RecordDeserializer(schema.getValueType(), fieldType);
        Object fieldValue = recordDes.visit(this, value);
        avroRecord.put(fromString(key.toString()), fieldValue);
    }

    private void processMapArray(BMap<BString, Object> avroRecord, Schema schema,
                                 MapType type, Object key, GenericData.Array<Object> value) throws Exception {
        Type fieldType = type.getConstrainedType();
        ArrayDeserializer arrayDeserializer = new ArrayDeserializer(schema.getValueType(), fieldType);
        Object fieldValue = visit(arrayDeserializer, value);
        avroRecord.put(fromString(key.toString()), fieldValue);
    }

    public Object visit(ArrayDeserializer arrayDeserializer, GenericData.Array<Object> data) throws Exception {
        Deserializer deserializer = createDeserializer(arrayDeserializer.getSchema(), arrayDeserializer.getType());
        return deserializer.visit(new DeserializeArrayVisitor(), data);
    }

    public BArray visit(EnumDeserializer enumDeserializer, GenericData.Array<Object> data) {
        Object[] enums = new Object[data.size()];
        for (int i = 0; i < data.size(); i++) {
            enums[i] = visitString(data.get(i));
        }
        return ValueCreator.createArrayValue(enums, (ArrayType) enumDeserializer.getType());
    }

    public Object visit(FixedDeserializer fixedDeserializer, Object data) {
        if (fixedDeserializer.getSchema().getType().equals(Schema.Type.ARRAY)) {
            GenericData.Array<Object> array = (GenericData.Array<Object>) data;
            Type type = fixedDeserializer.getType();
            List<BArray> values = new ArrayList<>();
            for (Object datum : array) {
                values.add(visitFixed(datum));
            }
            return ValueCreator.createArrayValue(values.toArray(new BArray[array.size()]), (ArrayType) type);
        } else {
            return visitFixed(data);
        }
    }

    public BArray visit(FixedDeserializer fixedDeserializer, GenericData.Array<Object> data) {
        Type type = fixedDeserializer.getType();
        List<BArray> values = new ArrayList<>();
        for (Object datum : data) {
            values.add(visitFixed(datum));
        }
        return ValueCreator.createArrayValue(values.toArray(new BArray[data.size()]), (ArrayType) type);
    }

    private static BArray visitIntegerArray(GenericData.Array<Object> data, Schema schema) {
        for (Schema schemaInstance : schema.getElementType().getTypes()) {
            if (schemaInstance.getType().equals(Schema.Type.INT)) {
                return visitIntArray(data);
            }
        }
        return visitLongArray(data);
    }

    private BArray visitBytesArray(GenericData.Array<Object> data, Type type) {
        List<BArray> values = new ArrayList<>();
        for (Object datum : data) {
            values.add(visitBytes(datum));
        }
        return ValueCreator.createArrayValue(values.toArray(new BArray[data.size()]), (ArrayType) type);
    }

    private static BArray visitBooleanArray(GenericData.Array<Object> data) {
        boolean[] booleanArray = new boolean[data.size()];
        int index = 0;
        for (Object datum : data) {
            booleanArray[index++] = (boolean) datum;
        }
        return ValueCreator.createArrayValue(booleanArray);
    }

    private BArray visitDoubleArray(GenericData.Array<Object> data) {
        List<Double> doubleList = new ArrayList<>();
        for (Object datum : data) {
            doubleList.add(visitDouble(datum));
        }
        double[] doubleArray = doubleList.stream().mapToDouble(Double::doubleValue).toArray();
        return ValueCreator.createArrayValue(doubleArray);
    }

    private static BArray visitLongArray(GenericData.Array<Object> data) {
        List<Long> longList = new ArrayList<>();
        for (Object datum : data) {
            longList.add((Long) datum);
        }
        long[] longArray = longList.stream().mapToLong(Long::longValue).toArray();
        return ValueCreator.createArrayValue(longArray);
    }

    private static BArray visitIntArray(GenericData.Array<Object> data) {
        List<Long> longList = new ArrayList<>();
        for (Object datum : data) {
            longList.add(((Integer) datum).longValue());
        }
        long[] longArray = longList.stream().mapToLong(Long::longValue).toArray();
        return ValueCreator.createArrayValue(longArray);
    }

    private BArray visitStringArray(GenericData.Array<Object> data) {
        BString[] stringArray = new BString[data.size()];
        for (int i = 0; i < data.size(); i++) {
            stringArray[i] = visitString(data.get(i));
        }
        return ValueCreator.createArrayValue(stringArray);
    }


    public double visitDouble(Object data) {
        if (data instanceof Float) {
            return Double.parseDouble(data.toString());
        }
        return (double) data;
    }

    public BArray visitBytes(Object data) {
        return ValueCreator.createArrayValue(((ByteBuffer) data).array());
    }

    public BArray visitFixed(Object data) {
        GenericData.Fixed fixed = (GenericData.Fixed) data;
        return ValueCreator.createArrayValue(fixed.bytes());
    }

    public BString visitString(Object data) {
        return fromString(data.toString());
    }

    public static Type extractMapType(Type type) throws Exception {
        Type mapType = type;
        if (type.getTag() != TypeTags.RECORD_TYPE_TAG) {
            throw new Exception("Type is not a record type.");
        }
        for (Map.Entry<String, Field> entry : ((RecordType) type).getFields().entrySet()) {
            Field fieldValue = entry.getValue();
            if (fieldValue != null) {
                Type fieldType = fieldValue.getFieldType();
                switch (fieldType.getTag()) {
                    case TypeTags.MAP_TAG ->
                            mapType = fieldType;
                    case TypeTags.INTERSECTION_TAG -> {
                        Type referredType = getMutableType((IntersectionType) fieldType);
                        if (referredType.getTag() == TypeTags.MAP_TAG) {
                            mapType = referredType;
                        }
                    }
                    default -> {
                        Type referType = TypeUtils.getReferredType(fieldType);
                        if (referType.getTag() == TypeTags.MAP_TAG) {
                            mapType = referType;
                        }
                    }
                }
            }
        }
        return mapType;
    }

    public static RecordType extractRecordType(RecordType type) {
        Map<String, Field> fieldsMap = type.getFields();
        RecordType recType = type;
        for (Map.Entry<String, Field> entry : fieldsMap.entrySet()) {
            Field fieldValue = entry.getValue();
            if (fieldValue != null) {
                Type fieldType = fieldValue.getFieldType();
                switch (fieldType.getTag()) {
                    case TypeTags.RECORD_TYPE_TAG ->
                            recType = (RecordType) fieldType;
                    case TypeTags.INTERSECTION_TAG -> {
                        Type getType = getMutableType(fieldType);
                        if (getType.getTag() == TypeTags.RECORD_TYPE_TAG) {
                            recType = (RecordType) getType;
                        }
                    }
                    default -> {
                        Type referredType = TypeUtils.getReferredType(fieldType);
                        if (referredType.getTag() == TypeTags.RECORD_TYPE_TAG) {
                            recType = (RecordType) referredType;
                        }
                    }
                }
            }
        }
        return recType;
    }
}

package io.ballerina.lib.avro.deserialize.visitor;

import io.ballerina.lib.avro.deserialize.ArrayDeserializer;
import io.ballerina.lib.avro.deserialize.MapDeserializer;
import io.ballerina.lib.avro.deserialize.RecordDeserializer;
import io.ballerina.lib.avro.deserialize.StringDeserializer;
import io.ballerina.runtime.api.creators.ValueCreator;
import io.ballerina.runtime.api.types.IntersectionType;
import io.ballerina.runtime.api.types.RecordType;
import io.ballerina.runtime.api.types.Type;
import io.ballerina.runtime.api.utils.StringUtils;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BString;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.nio.ByteBuffer;

import static io.ballerina.lib.avro.Utils.getMutableType;
import static io.ballerina.lib.avro.deserialize.visitor.UnionRecordUtils.visitUnionRecords;

public class DeserializeRecordVisitor extends DeserializeVisitor {

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
                        avroRecord.put(StringUtils.fromString(field.name()), Long.parseLong(fieldData.toString()));
                case FLOAT ->
                        avroRecord.put(StringUtils.fromString(field.name()), Double.parseDouble(fieldData.toString()));
                case UNION ->
                        processUnionField(type, avroRecord, field, fieldData);
                default ->
                        avroRecord.put(StringUtils.fromString(field.name()), fieldData);
            }
        }

        if (originalType.isReadOnly()) {
            avroRecord.freezeDirect();
        }
        return avroRecord;
    }

    private BMap<BString, Object> createAvroRecord(Type type) {
        if (type instanceof IntersectionType) {
            type = getMutableType((IntersectionType) type);
        }
        return ValueCreator.createRecordValue((RecordType) type);
    }

    private void processMapField(BMap<BString, Object> avroRecord,
                                 Schema.Field field, Object fieldData) throws Exception {
        Type mapType = extractMapType(avroRecord.getType());
        MapDeserializer mapDeserializer = new MapDeserializer(field.schema(), mapType);
        Object fieldValue = mapDeserializer.visit(this, fieldData);
        avroRecord.put(StringUtils.fromString(field.name()), fieldValue);
    }

    private void processArrayField(BMap<BString, Object> avroRecord,
                                   Schema.Field field, Object fieldData) throws Exception {
        ArrayDeserializer arrayDes = new ArrayDeserializer(field.schema(), avroRecord.getType());
        Object fieldValue = arrayDes.visit(this, (GenericData.Array<Object>) fieldData);
        avroRecord.put(StringUtils.fromString(field.name()), fieldValue);
    }

    private void processBytesField(BMap<BString, Object> avroRecord, Schema.Field field, Object fieldData) {
        ByteBuffer byteBuffer = (ByteBuffer) fieldData;
        Object fieldValue = ValueCreator.createArrayValue(byteBuffer.array());
        avroRecord.put(StringUtils.fromString(field.name()), fieldValue);
    }

    private void processRecordField(BMap<BString, Object> avroRecord,
                                    Schema.Field field, Object fieldData) throws Exception {
        Type recType = extractRecordType((RecordType) avroRecord.getType());
        RecordDeserializer recordDes = new RecordDeserializer(field.schema(), recType);
        Object fieldValue = recordDes.visit(this, (GenericRecord) fieldData);
        avroRecord.put(StringUtils.fromString(field.name()), fieldValue);
    }

    private void processStringField(BMap<BString, Object> avroRecord,
                                    Schema.Field field, Object fieldData) throws Exception {
        StringDeserializer stringDes = new StringDeserializer();
        Object fieldValue = stringDes.visit(this, fieldData);
        avroRecord.put(StringUtils.fromString(field.name()), fieldValue);
    }

    private void processUnionField(Type type, BMap<BString, Object> avroRecord,
                                   Schema.Field field, Object fieldData) throws Exception {
        visitUnionRecords(type, avroRecord, field, fieldData);
    }
}

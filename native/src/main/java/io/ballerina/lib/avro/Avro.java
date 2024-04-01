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

package io.ballerina.lib.avro;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.avro.AvroMapper;
import com.fasterxml.jackson.dataformat.avro.AvroSchema;
import io.ballerina.runtime.api.creators.ValueCreator;
import io.ballerina.runtime.api.utils.JsonUtils;
import io.ballerina.runtime.api.utils.StringUtils;
import io.ballerina.runtime.api.utils.ValueUtils;
import io.ballerina.runtime.api.values.BArray;
import io.ballerina.runtime.api.values.BHandle;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BString;
import io.ballerina.runtime.api.values.BTypedesc;
import org.apache.avro.Schema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Objects;

import static io.ballerina.lib.avro.ModuleUtils.AVRO_SCHEMA;

public class Avro {

    private Schema nativeSchema;

    public Avro(BString schema) {
        Schema.Parser parser = new Schema.Parser();
        this.nativeSchema = parser.parse(schema.getValue());
    }

    protected Schema getSchema() {
        return nativeSchema;
    }

    protected void setSchema(Schema nativeSchema) {
        this.nativeSchema = nativeSchema;
    }

    public static Object toAvro(BObject avroObject, Object data) {
        BHandle handle = (BHandle) avroObject.get(StringUtils.fromString(AVRO_SCHEMA));
        Avro avro = (Avro) handle.getValue();
        Schema schema = avro.getSchema();
        ObjectMapper objectMapper = new ObjectMapper();
        Object jsonObject;
        try {
            jsonObject = generateJsonObject(data, schema, objectMapper);
            if (jsonObject == null) {
                return ValueCreator.createArrayValue(new byte[]{0});
            } else if (Objects.equals(schema.getType(), Schema.Type.FIXED)) {
                return ValueCreator.createArrayValue(((BArray) jsonObject).getByteArray());
            }

            applyByteValuesToJsonString(data, schema, jsonObject);

            byte[] avroBytes = (new AvroMapper()).writer(new AvroSchema(schema)).writeValueAsBytes(jsonObject);
            return ValueCreator.createArrayValue(avroBytes);
        } catch (JsonProcessingException e) {
            return Utils.createError(e.getMessage(), Utils.createError(e.getCause().getMessage()));
        }
    }

    private static void applyByteValuesToJsonString(Object bData, Schema schema, Object jsonObject) {
        ArrayList<String> byteFields = new ArrayList<>();
        if (schema.getType() == Schema.Type.RECORD) {
            for (Schema.Field field : schema.getFields()) {
                if (Objects.equals(field.schema().getType(), Schema.Type.BYTES)) {
                    byteFields.add(field.name());
                }
            }
        }
        if (byteFields.isEmpty()) {
            return;
        }
        LinkedHashMap<?, BArray> data = (LinkedHashMap<?, BArray>) bData;
        LinkedHashMap hashedMap = (LinkedHashMap<?, ?>) jsonObject;
        for (String fieldName: byteFields) {
            hashedMap.put(fieldName, data.get(StringUtils.fromString(fieldName)).getByteArray());
        }
    }

    public static Object fromAvro(BObject avroObject, BArray payload, BTypedesc typeParam) {
        AvroMapper mapper = new AvroMapper();
        BHandle handle = (BHandle) avroObject.get(StringUtils.fromString(AVRO_SCHEMA));
        Avro avro = (Avro) handle.getValue();
        Schema schema = avro.getSchema();
        byte[] avroBytes = payload.getByteArray();
        if (schema.getType() == Schema.Type.FIXED) {
            return ValueUtils.convert(ValueCreator.createArrayValue(avroBytes), typeParam.getDescribingType());
        }
        JsonNode deserializedJsonString;
        try {
            deserializedJsonString = mapper.readerFor(Object.class).with(new AvroSchema(schema)).readTree(avroBytes);
        } catch (IOException e) {
            return Utils.createError(e.getMessage(), Utils.createError(e.getCause().getMessage()));
        }
        Object jsonObject = JsonUtils.parse(deserializedJsonString.toPrettyString());
        applyByteValueToDeserializeData(schema, jsonObject);

        return ValueUtils.convert(jsonObject, typeParam.getDescribingType());
    }

    private static void applyByteValueToDeserializeData(Schema schema, Object jsonObject) {
        ArrayList<String> byteFields = new ArrayList<>();
        if (schema.getType() == Schema.Type.RECORD) {
            for (Schema.Field field : schema.getFields()) {
                if (Objects.equals(field.schema().getType(), Schema.Type.BYTES)) {
                    byteFields.add(field.name());
                }
            }
        }
        if (byteFields.isEmpty()) {
            return;
        }
        LinkedHashMap hashedMap = (LinkedHashMap<?, ?>) jsonObject;
        for (String fieldName: byteFields) {
            byte[] values = Base64.getDecoder().decode(((LinkedHashMap<?, BString>) jsonObject)
                    .get(StringUtils.fromString(fieldName)).getValue());
            BArray byteArray = ValueCreator.createArrayValue(values);
            hashedMap.put(StringUtils.fromString(fieldName), byteArray);
        }
    }

    private static Object generateJsonObject(Object data, Schema schema,
                                             ObjectMapper objectMapper) throws JsonProcessingException {
        Object jsonObject;
        if (Objects.equals(schema.getType(), Schema.Type.NULL)
            || Objects.equals(schema.getType(), Schema.Type.FIXED)) {
            jsonObject = data;
        } else if (Objects.equals(schema.getType(), Schema.Type.STRING) ||
                   Objects.equals(schema.getType(), Schema.Type.ENUM)) {
            jsonObject = objectMapper.readValue("\"" + data + "\"", Object.class);
        } else {
            Object jsonString = JsonUtils.parse(StringUtils.getJsonString(data));
            jsonObject = objectMapper.readValue(jsonString.toString(), Object.class);
        }
        return jsonObject;
    }
}

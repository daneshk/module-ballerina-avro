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
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BString;
import io.ballerina.runtime.api.values.BTypedesc;
import org.apache.avro.Schema;

import java.io.IOException;
import java.util.Objects;

import static io.ballerina.lib.avro.Utils.AVRO_SCHEMA;
import static io.ballerina.lib.avro.Utils.DESERIALIZATION_ERROR;
import static io.ballerina.lib.avro.Utils.JSON_PROCESSING_ERROR;

public final class Avro {

    private Avro() {}

    public static void generateSchema(BObject schemaObject, BString schema) {
        Schema.Parser parser = new Schema.Parser();
        Schema nativeSchema = parser.parse(schema.getValue());
        schemaObject.addNativeData(AVRO_SCHEMA, nativeSchema);
    }

    public static Object toAvro(BObject schemaObject, Object data) {
        Schema schema = (Schema) schemaObject.getNativeData(AVRO_SCHEMA);
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            Object jsonObject = generateJsonObject(data, schema, objectMapper);
            if (jsonObject == null) {
                return ValueCreator.createArrayValue(new byte[]{0, 0});
            } else if (Objects.equals(schema.getType(), Schema.Type.FIXED)) {
                return ValueCreator.createArrayValue(((BArray) jsonObject).getByteArray());
            }
            byte[] avroBytes = (new AvroMapper()).writer(new AvroSchema(schema)).writeValueAsBytes(jsonObject);
            return ValueCreator.createArrayValue(avroBytes);
        } catch (JsonProcessingException e) {
            return Utils.createError(JSON_PROCESSING_ERROR, e);
        }
    }

    public static Object fromAvro(BObject schemaObject, BArray payload, BTypedesc typeParam) {
        Schema schema = (Schema) schemaObject.getNativeData(AVRO_SCHEMA);
        byte[] avroBytes = payload.getByteArray();
        if (Schema.Type.FIXED.equals(schema.getType())) {
            return ValueUtils.convert(ValueCreator.createArrayValue(avroBytes), typeParam.getDescribingType());
        }
        JsonNode deserializedJsonString;
        try {
            AvroMapper mapper = new AvroMapper();
            deserializedJsonString = mapper.readerFor(Object.class).with(new AvroSchema(schema)).readTree(avroBytes);
        } catch (IOException e) {
            return Utils.createError(DESERIALIZATION_ERROR, e);
        }
        Object jsonObject = JsonUtils.parse(deserializedJsonString.toPrettyString());
        return ValueUtils.convert(jsonObject, typeParam.getDescribingType());
    }

    private static Object generateJsonObject(Object data, Schema schema,
                                             ObjectMapper objectMapper) throws JsonProcessingException {
        if (Schema.Type.NULL.equals(schema.getType()) || Schema.Type.FIXED.equals(schema.getType())) {
            return data;
        } else if (Schema.Type.STRING.equals(schema.getType()) || Schema.Type.ENUM.equals(schema.getType())) {
            return objectMapper.readValue("\"" + data + "\"", Object.class);
        }
        Object jsonString = JsonUtils.parse(StringUtils.getJsonString(data));
        return objectMapper.readValue(jsonString.toString(), Object.class);
    }
}

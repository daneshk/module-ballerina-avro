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


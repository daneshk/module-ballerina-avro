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

import java.util.Map;

import static java.util.Map.entry;

public class UnionArrayVisitor implements IArrayVisitor {
    @Override
    public GenericData.Array<Object> visit(BArray data, Schema schema, GenericData.Array<Object> array) {
        Map<Schema.Type, IArrayVisitor> visitorMap = Map.ofEntries(
                entry(Schema.Type.ARRAY, new ArrayVisitor()),
                entry(Schema.Type.MAP, new MapArrayVisitor()),
                entry(Schema.Type.RECORD, new RecordArrayVisitor()),
                entry(Schema.Type.FIXED, new FixedArrayVisitor()),
                entry(Schema.Type.BOOLEAN, new PrimitiveArrayVisitor()),
                entry(Schema.Type.STRING, new PrimitiveArrayVisitor()),
                entry(Schema.Type.INT, new PrimitiveArrayVisitor()),
                entry(Schema.Type.LONG, new PrimitiveArrayVisitor()),
                entry(Schema.Type.DOUBLE, new PrimitiveArrayVisitor()),
                entry(Schema.Type.BYTES, new PrimitiveArrayVisitor()),
                entry(Schema.Type.FLOAT, new PrimitiveArrayVisitor())
        );

        Schema elementType = schema.getElementType();
        for (Schema schema1 : elementType.getTypes()) {
            IArrayVisitor visitor = visitorMap.get(schema1.getType());
            if (visitor != null) {
                return visitor.visit(data, schema1, array);
            }
        }
        return null;
    }
}

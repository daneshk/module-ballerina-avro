package io.ballerina.lib.avro.deserialize.visitor;

import io.ballerina.lib.avro.deserialize.ArrayDeserializer;
import io.ballerina.lib.avro.deserialize.Deserializer;
import io.ballerina.runtime.api.creators.ValueCreator;
import io.ballerina.runtime.api.types.ArrayType;
import io.ballerina.runtime.api.types.Type;
import org.apache.avro.generic.GenericData;

public class DeserializeArrayVisitor extends DeserializeVisitor {

    public Object visit(ArrayDeserializer arrayDeserializer, GenericData.Array<Object> data) throws Exception {
        Object[] objects = new Object[data.size()];
        Type arrayType = ((ArrayType) arrayDeserializer.getType()).getElementType();
        int index = 0;
        for (Object element : data) {
            GenericData.Array<Object> dataArray = (GenericData.Array<Object>) element;
            Type arrType = arrayType instanceof ArrayType ? arrayType : arrayDeserializer.getType();
            objects[index++] = visitNestedArray(new ArrayDeserializer(arrayDeserializer.getSchema().getElementType(),
                    arrType), dataArray);
        }
        return ValueCreator.createArrayValue(objects, (ArrayType) arrayDeserializer.getType());
    }

    public Object visitNestedArray(ArrayDeserializer arrayDeserializer,
                                   GenericData.Array<Object> data) throws Exception {
        Deserializer deserializer = createDeserializer(arrayDeserializer.getSchema(), arrayDeserializer.getType());
        return deserializer.visit(this, data);
    }
}

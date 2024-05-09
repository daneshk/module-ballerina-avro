package io.ballerina.lib.avro.serialize.visitor.array;

import io.ballerina.runtime.api.values.BArray;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;

public interface IArrayVisitor {
    GenericData.Array<Object> visit(BArray data, Schema schema, GenericData.Array<Object> array);
}

package org.apache.arrow.vector;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;

public class TestUtils {

    public static VarCharVector newVarCharVector(String name, BufferAllocator allocator) {
        return (VarCharVector)
                FieldType.nullable(new ArrowType.Utf8()).createNewSingleVector(name, allocator, null);
    }

    public static VarBinaryVector newVarBinaryVector(String name, BufferAllocator allocator) {
        return (VarBinaryVector)
                FieldType.nullable(new ArrowType.Binary()).createNewSingleVector(name, allocator, null);
    }

    public static <T> T newVector(Class<T> c, String name, ArrowType type, BufferAllocator allocator) {
        return c.cast(FieldType.nullable(type).createNewSingleVector(name, allocator, null));
    }

    public static <T> T newVector(Class<T> c, String name, MinorType type, BufferAllocator allocator) {
        return c.cast(FieldType.nullable(type.getType()).createNewSingleVector(name, allocator, null));
    }
}

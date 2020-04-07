package org.apache.arrow.vector.testing;

import org.apache.arrow.vector.VarCharVector;

// 这个类是配合官方测试类使用的，没有被编译进jar包里
//需要在arrow-demo工程中新建一个包org.apache.arrow.vector.testing
// 用于生成相关数据{@link org.apache.arrow.vector.ValueVector}
public class ValueVectorDataPopulator {

    public static void setVector(VarCharVector vector, byte[]... values) {
        final int length = values.length;
        vector.allocateNewSafe();
        for(int i=0;i<length;i++) {
            if(values[i] != null) {
                vector.set(i, values[i]);
            }
        }
        vector.setValueCount(length);
    }
}

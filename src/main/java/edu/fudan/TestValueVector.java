package edu.fudan;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestValueVector {
    private RootAllocator allocator;

    @Before
    public void startAllocator() {
        allocator = new RootAllocator(Long.MAX_VALUE);
    }

    @After
    public void closeAllocator() {
        allocator.close();
    }

    @Test
    public void testValueVector() {
        //创建
        IntVector vector = new IntVector("int vector", allocator);
        //分配
        vector.allocateNew(10);
        //填值
        vector.set(/*index*/5, /*value*/25);
        //计数器
        vector.setValueCount(10);
        //访问
        int value = vector.get(5);
        System.out.println("value = " + value);
        System.out.println("vector = " + vector);
        //关闭
        vector.close();
    }
}

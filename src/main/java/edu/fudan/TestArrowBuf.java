package edu.fudan;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.nio.ByteBuffer;
import java.util.Arrays;

import io.netty.buffer.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.*;

public class TestArrowBuf {
    private static final int MAX_ALLOCATION = 8*1024;
    private static RootAllocator allocator;

    @BeforeClass
    public static void beforeClass() {
        allocator = new RootAllocator(MAX_ALLOCATION);
    }

    @AfterClass
    public static void afterClass() {
        if(allocator!=null) {
            allocator.close();
        }
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testSliceOutOfBoundsLength_RaisesIndexOutOfBoundsException() {
        //测试给buffer分配的空间

        try(BufferAllocator allocator = new RootAllocator(128);
            ArrowBuf buf = allocator.buffer(2)
        ) {
            assertEquals(2, buf.capacity());
            //返回一个切片，从索引0开始，长度为3
            buf.slice(0, 3);
        }
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testSliceOutOfBoundsIndexPlusLength_RaisesIndexOutOfBoundsException() {
        try(BufferAllocator allocator = new RootAllocator(128);
        ArrowBuf buf = allocator.buffer(2)
        ) {
            assertEquals(2, buf.capacity());
            buf.slice(1, 2);
        }
    }

    @Test
    public void testSetBytesSliced() {
        //测试buffer赋值

        int arrLength = 64;
        byte[] expected = new byte[arrLength];
        for(int i=0;i<expected.length;i++) {
            expected[i] = (byte) i;
            System.out.println(expected[i]);
        }
        ByteBuffer data = ByteBuffer.wrap(expected);
        try(ArrowBuf buf = allocator.buffer(expected.length)) {
            buf.setBytes(0, data, 0, data.capacity());
            byte[] actual = new byte[expected.length];
            buf.getBytes(0, actual);
            assertArrayEquals(expected, actual);
        }
    }
}

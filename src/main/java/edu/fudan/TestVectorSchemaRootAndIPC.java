package edu.fudan;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

public class TestVectorSchemaRootAndIPC {
    private RootAllocator allocator;

    @Before
    public void initAllocator() {
        allocator = new RootAllocator(Long.MAX_VALUE);
    }

    @After
    public void closeAllocator() {
        allocator.close();
    }

    @Test
    public void testVectorSchemaRootAndIPC() throws Exception {

        //设置ArrowStreamWriter的provider
        DictionaryProvider.MapDictionaryProvider provider =
                new DictionaryProvider.MapDictionaryProvider();

        //新建两个不同类型的vector
        BitVector bitVector = new BitVector("boolean", allocator);
        VarCharVector varCharVector = new VarCharVector("varchar", allocator);

        //给其填充值
        for(int i=0;i<10;i++) {
            bitVector.setSafe(i, i % 2 == 0 ? 0 : 1);
            varCharVector.setSafe(i, ("test" + i).getBytes(StandardCharsets.UTF_8));
        }

        //设置count
        bitVector.setValueCount(10);
        varCharVector.setValueCount(10);

        //封装进VectorSchemaRoot流水线
        List<Field> fields = Arrays.asList(bitVector.getField(), varCharVector.getField());
        List<FieldVector> vectors = Arrays.asList(bitVector, varCharVector);
        VectorSchemaRoot root = new VectorSchemaRoot(fields, vectors);

        System.out.println("bitVector: " + bitVector);
        System.out.println("varCharVector: " + varCharVector);
        bitVector.close();
        varCharVector.close();

        //流水线写进输出流
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ArrowStreamWriter writer = new ArrowStreamWriter(root,
                /*DictionaryProvider=*/provider, Channels.newChannel(out));

        writer.start();
        //写第一个batch
        writer.writeBatch();
        //写另外四个batch
        for(int i=0;i<4;i++) {
            //生成VectorSchemaRoot数据和写第二个batch
            BitVector childVector1 = (BitVector)root.getVector(0);
            VarCharVector childVector2 = (VarCharVector)root.getVector(1);
            System.out.println("childVector1: " + childVector1);
            System.out.println("childVector2: " + childVector2);
            childVector1.reset();
            childVector2.reset();
            writer.writeBatch();
        }

        //end
        writer.end();

        //需要在writeBatch之前生成数据，因此之后的batches可以重写之前的batches
        //ByteArrayOutputStream包含完整的流（内含5个记录batches），可用ArrowStreamReader读这个流
        //含有reader的VectorSchemaRoot会用loadNextBatch()加载到每一次的新的值
        try(ArrowStreamReader reader = new ArrowStreamReader(new ByteArrayInputStream(out.toByteArray()), allocator)) {
            Schema schema = reader.getVectorSchemaRoot().getSchema();
            for(int i=0;i<5;i++) {
                //每一次调用loadNextBatch会加载到新的值
                VectorSchemaRoot readBatch = reader.getVectorSchemaRoot();
                reader.loadNextBatch();
            }
        }
    }
}

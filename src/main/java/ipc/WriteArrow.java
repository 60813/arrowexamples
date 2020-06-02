package ipc;

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
import org.junit.AfterClass;
import org.junit.Before;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Dictionary;
import java.util.List;

//往伪共享内存中写入文件
public class WriteArrow {



    public static void main(String[] args) throws Exception {

        RootAllocator allocator;
        allocator = new RootAllocator(Long.MAX_VALUE);

        //设置ArrowStreamWriter的provider
        DictionaryProvider.MapDictionaryProvider provider =
                new DictionaryProvider.MapDictionaryProvider();

        //新建Bit类型和VarChar类型的vector
        VarCharVector varCharVector = new VarCharVector("varchar", allocator);

        //给其填充值
        for(int i=0;i<10;i++) {
            varCharVector.setSafe(i, ("test" + i).getBytes(StandardCharsets.UTF_8));
        }

        //设置count
        varCharVector.setValueCount(10);

        //封装进VectorSchemaRoot流水线
        List<Field> fields = Arrays.asList(varCharVector.getField());
        List<FieldVector> vectors = Arrays.asList(varCharVector);
        VectorSchemaRoot root = new VectorSchemaRoot(fields, vectors);

//        System.out.println("varCharVector: " + varCharVector);

        long time1 = System.nanoTime();

        //流水线写进输出线
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ArrowStreamWriter writer = new ArrowStreamWriter(root, provider, Channels.newChannel(out));

        writer.start();
        writer.writeBatch();

        try(ArrowStreamReader reader = new ArrowStreamReader(new ByteArrayInputStream(out.toByteArray()), allocator)) {
            Schema schema = reader.getVectorSchemaRoot().getSchema();
            VectorSchemaRoot readBatch = reader.getVectorSchemaRoot();
//            System.out.println("readBatch.getSchema: " + readBatch.getSchema());
//            System.out.println("readBatch.getFieldVectors: " + readBatch.getFieldVectors());
//            System.out.println("readBatch.getRowCount: " + readBatch.getRowCount());
//            System.out.println("readBatch.contentToTSVString: " + readBatch.contentToTSVString());

            long time2 = System.nanoTime();
            System.out.println("Schema耗时：" + (time2 - time1) + "ns");

            reader.loadNextBatch();
            readBatch = reader.getVectorSchemaRoot();

//            System.out.println("readBatch.getSchema: " + readBatch.getSchema());
//            System.out.println("readBatch.getFieldVectors: " + readBatch.getFieldVectors());
//            System.out.println("readBatch.getRowCount: " + readBatch.getRowCount());
//            System.out.println("readBatch.contentToTSVString: " + readBatch.contentToTSVString());
            long time3 = System.nanoTime();
            System.out.println("RB耗时：" + (time3 - time2) + "ns");
        }

        writer.close();
        varCharVector.close();

        long timeCommon1 = System.nanoTime();

        String [] var2 = new String[] {"test0","test1","test2","test3","test4","test5","test6","test7","test8","test9",};
        ByteArrayOutputStream out2 = new ByteArrayOutputStream();
        for(int j=0;j<var2.length;j++){
            out2.write(var2[j].getBytes());
        }
        byte[] out2b = out2.toByteArray();
        ByteArrayInputStream bInput = new ByteArrayInputStream(out2b);
        int c;
        while(( c= bInput.read())!= -1) {
            System.out.print((char)c);
        }
        bInput.reset();

        long timeCommon2 = System.nanoTime();
        System.out.println("普通耗时：" + (timeCommon2 - timeCommon1) + "ns");

    }
}

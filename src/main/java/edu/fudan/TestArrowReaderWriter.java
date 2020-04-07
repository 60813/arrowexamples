package edu.fudan;

import static java.nio.channels.Channels.newChannel;
import static java.util.Arrays.asList;
import static org.apache.arrow.memory.util.LargeMemoryUtil.checkedCastToInt;
import static org.apache.arrow.vector.testing.ValueVectorDataPopulator.setVector;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import io.netty.buffer.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.Collections2;
import org.apache.arrow.vector.*;
//import org.apache.arrow.vector.TestUtils;
import org.apache.arrow.vector.dictionary.DictionaryEncoder;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.SeekableReadChannel;
import org.apache.arrow.vector.ipc.message.ArrowBlock;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import org.apache.arrow.vector.util.ByteArrayReadableSeekableByteChannel;
import org.junit.After;
import org.junit.Before;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.Channels;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.arrow.vector.dictionary.Dictionary;
import org.junit.Test;

//官方测试类
public class TestArrowReaderWriter {

    private BufferAllocator allocator;

    private VarCharVector dictionaryVector1;
    private VarCharVector dictionaryVector2;
    private VarCharVector dictionaryVector3;

    private Dictionary dictionary1;
    private Dictionary dictionary2;
    private Dictionary dictionary3;

    private Schema schema;
    private Schema encodedSchema;

    @Before
    public void init() {
        allocator = new RootAllocator(Long.MAX_VALUE);

        dictionaryVector1 = new VarCharVector("D1", allocator);
        setVector(dictionaryVector1,
                "foo".getBytes(StandardCharsets.UTF_8),
                "bar".getBytes(StandardCharsets.UTF_8),
                "baz".getBytes(StandardCharsets.UTF_8)
                );

        dictionaryVector2 = new VarCharVector("D2", allocator);
        setVector(dictionaryVector2,
                "aa".getBytes(StandardCharsets.UTF_8),
                "bb".getBytes(StandardCharsets.UTF_8),
                "cc".getBytes(StandardCharsets.UTF_8));

        dictionaryVector3 = new VarCharVector("D3", allocator);
        setVector(dictionaryVector3,
                "foo".getBytes(StandardCharsets.UTF_8),
                "bar".getBytes(StandardCharsets.UTF_8),
                "baz".getBytes(StandardCharsets.UTF_8),
                "aa".getBytes(StandardCharsets.UTF_8),
                "bb".getBytes(StandardCharsets.UTF_8),
                "cc".getBytes(StandardCharsets.UTF_8));

        dictionary1 = new Dictionary(dictionaryVector1,
                new DictionaryEncoding(/*id=*/1L, /*ordered=*/false, /*indexType=*/null));
        dictionary2 = new Dictionary(dictionaryVector2,
                new DictionaryEncoding(/*id=*/2L, /*ordered=*/false, /*indexType=*/null));
        dictionary3 = new Dictionary(dictionaryVector3,
                new DictionaryEncoding(/*id=*/1L, /*ordered=*/false, /*indexType=*/null));
    }

    @After
    public void terminate() throws Exception {
        dictionaryVector1.close();
        dictionaryVector2.close();
        dictionaryVector3.close();
        allocator.close();
    }

    //开辟一个ArrowBuffer并存进字节流bytes
    ArrowBuf buf(byte[] bytes) {
        ArrowBuf buffer = allocator.buffer(bytes.length);
        buffer.writeBytes(bytes);
        return buffer;
    }

    byte[] array(ArrowBuf buf) {
        byte[] bytes = new byte[checkedCastToInt(buf.readableBytes())];
        buf.readBytes(bytes);
        return bytes;
    }

    @Test
    public void testWriteReadNullVector() throws IOException {
        // 测试NullVector的读写，NullVector是一个null型的vector

        int valueCount = 3;
        NullVector nullVector = new NullVector();
        nullVector.setValueCount(valueCount);

        // Schema描述了二维数据集（如表格）的整体结构。
        Schema schema = new Schema(asList(nullVector.getField()));

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try(VectorSchemaRoot root = new VectorSchemaRoot(schema.getFields(), asList(nullVector), valueCount);
            // 调用writer将schema等写输出流
            ArrowFileWriter writer = new ArrowFileWriter(root, null, newChannel(out))) {
            ArrowRecordBatch batch = new ArrowRecordBatch(valueCount,
                    asList(new ArrowFieldNode(valueCount, 0)),
                    Collections.emptyList());
            VectorLoader loader = new VectorLoader(root);

            // 装载批数据
            loader.load(batch);
            writer.writeBatch();
        }

        byte[] byteArray = out.toByteArray();

        try(SeekableReadChannel channel = new SeekableReadChannel(new ByteArrayReadableSeekableByteChannel(byteArray));
            ArrowFileReader reader = new ArrowFileReader(channel, allocator)) {
            Schema readSchema = reader.getVectorSchemaRoot().getSchema();

            // 从输出流读取到的readSchema和初始定义的schema应该是一样的
            assertEquals(schema, readSchema);
            // batch只有一个（没有调用for循环之类的给writer循环赋输出流），因此size等于1
            List<ArrowBlock> recordBatches = reader.getRecordBlocks();
            assertEquals(1, recordBatches.size());

            assertTrue(reader.loadNextBatch());
            System.out.println(reader.getVectorSchemaRoot().getFieldVectors().size()); // 1
            assertEquals(1, reader.getVectorSchemaRoot().getFieldVectors().size());

            NullVector readNullVector = (NullVector) reader.getVectorSchemaRoot().getFieldVectors().get(0);
            System.out.println(readNullVector.getValueCount()); // 3
            assertEquals(valueCount, readNullVector.getValueCount());
        }
    }

    @Test
    public void testWriteReadWithDictionaries() throws IOException {
        // 测试字典类型的读写
        DictionaryProvider.MapDictionaryProvider provider = new DictionaryProvider.MapDictionaryProvider();
        provider.put(dictionary1);

        // 新建一个VarCharVector类型，用safe法分配空间
        VarCharVector vector1 = new VarCharVector("varchar1", allocator);
        vector1.allocateNewSafe();
        vector1.set(0, "foo".getBytes(StandardCharsets.UTF_8));
        vector1.set(1, "bar".getBytes(StandardCharsets.UTF_8));
        vector1.set(3, "baz".getBytes(StandardCharsets.UTF_8));
        vector1.set(4, "bar".getBytes(StandardCharsets.UTF_8));
        vector1.set(5, "baz".getBytes(StandardCharsets.UTF_8));
        vector1.setValueCount(6);

        // 用dictionary1的数值排列给vector1对象编码
        FieldVector encodedVector1 = (FieldVector) DictionaryEncoder.encode(vector1, dictionary1);
        vector1.close();

        // vector2同vector1方法处理
        VarCharVector vector2 = new VarCharVector("varchar2", allocator);
        vector2.allocateNewSafe();
        vector2.set(0, "bar".getBytes(StandardCharsets.UTF_8));
        vector2.set(1, "baz".getBytes(StandardCharsets.UTF_8));
        vector2.set(2, "foo".getBytes(StandardCharsets.UTF_8));
        vector2.set(3, "foo".getBytes(StandardCharsets.UTF_8));
        vector2.set(4, "foo".getBytes(StandardCharsets.UTF_8));
        vector2.set(5, "bar".getBytes(StandardCharsets.UTF_8));
        vector2.setValueCount(6);
        FieldVector encodedVector2 = (FieldVector) DictionaryEncoder.encode(vector2, dictionary1);
        vector2.close();

        // Field用于表示表的特定列
        List<Field> fields = Arrays.asList(encodedVector1.getField(), encodedVector2.getField());
        System.out.println(encodedVector1.getField()); // varchar1: Int(32, true)[dictionary: 1]
        System.out.println(encodedVector2.getField()); // varchar2: Int(32, true)[dictionary: 1]
        List<FieldVector> vectors = Collections2.asImmutableList(encodedVector1, encodedVector2);
        System.out.println("vectors: " + vectors); // vectors: [[0, 1, null, 2, 1, 2], [1, 2, 0, 0, 0, 1]]

        try (VectorSchemaRoot root = new VectorSchemaRoot(fields, vectors, encodedVector1.getValueCount());
             ByteArrayOutputStream out = new ByteArrayOutputStream();
             ArrowFileWriter writer = new ArrowFileWriter(root, provider, newChannel(out));) {

            writer.start();
            writer.writeBatch();
            writer.end();

            try (SeekableReadChannel channel = new SeekableReadChannel(
                    new ByteArrayReadableSeekableByteChannel(out.toByteArray()));
                 ArrowFileReader reader = new ArrowFileReader(channel, allocator)) {
                // 从输入流中获取readSchema
                Schema readSchema = reader.getVectorSchemaRoot().getSchema();
                // VectorSchemaRoot管道不出故障的话则readSchema和原始定义的root相同
                assertEquals(root.getSchema(), readSchema);
                assertEquals(1, reader.getDictionaryBlocks().size());
                assertEquals(1, reader.getRecordBlocks().size());

                reader.loadNextBatch();
                assertEquals(2, reader.getVectorSchemaRoot().getFieldVectors().size());
            }
        }
    }

    @Test
    public void testChannelReadFully() throws IOException {
        final ByteBuffer buf = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
        // 以当前字节顺序将包含给定int值（200）的四个字节写入当前位置的此缓冲区中，然后将该位置加四。（int占四字节）
        buf.putInt(200);
        // rewind（倒带？）这个缓冲区。position设置为零，mark设置为-1（丢弃）。
        buf.rewind();

        try(ReadChannel channel = new ReadChannel(Channels.newChannel(new ByteArrayInputStream(buf.array())));
            // 分配8字节缓冲区
            ArrowBuf arrBuf = allocator.buffer(8)) {
            // 在ArrowBuf可以访问的基础内存块中的特定索引处（0）设置int值（100）。
            arrBuf.setInt(0, 100);
            // 在writerIndex处设置提供的int值（4）。
            arrBuf.writerIndex(4);
            assertEquals(4, arrBuf.writerIndex());

            // 读取len到缓冲区，返回读取的字节。
            // arrBuf是要读的缓冲区，3是要读的字节数
            // channel类是读入ArrowBufs的ReadableByteChannel周围的适配器。
            long n = channel.readFully(arrBuf, 3);
            assertEquals(3, n);
            assertEquals(7, arrBuf.writerIndex()); // 此处的值不能超过上面给arrBuf分配的空间8字节

            // 获取存储在ArrowBuf可以访问的基础内存块中特定索引处的int值。
            // 上面在索引0处设置了值100
            assertEquals(100, arrBuf.getInt(0));
            // 至于索引4处的值是200，则是因为一开始的buf.putInt(200);
            assertEquals(200, arrBuf.getInt(4));
        }
    }
}

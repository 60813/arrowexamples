package ipc;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.message.ArrowBlock;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.ByteArrayReadableSeekableByteChannel;

import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

//从伪共享内存中读数据
public class ReadArrow {

    //普通输入流
    public static void inputStream(Path filename) {
        try(InputStream is = Files.newInputStream(filename)) {
            int c;
            while((c = is.read()) != -1) {

            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //带缓冲的输入流
    public static void bufferedInputStream(Path filename) {
        try(InputStream is = new BufferedInputStream(Files.newInputStream(filename))) {
            int c;
            while((c = is.read()) != -1) {

            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //随机访问文件
    public static void randomAccessFile(Path filename) {
        try(RandomAccessFile randomAccessFile = new RandomAccessFile(filename.toFile(), "r")) {
            for(long i=0;i<randomAccessFile.length();i++) {
                randomAccessFile.seek(i);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //内存映射文件
    public static void mappedFile(Path filename) {
        try(FileChannel fileChannel = FileChannel.open(filename)) {
            long size = fileChannel.size();
            MappedByteBuffer mappedByteBuffer = fileChannel.map(MapMode.READ_ONLY, 0, size);
            for(int i=0;i<size;i++) {
                mappedByteBuffer.get(i);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String [] args) throws Exception{

        RootAllocator allocator;
        allocator = new RootAllocator(Long.MAX_VALUE);

        //新建VarChar类型的vector
        VarCharVector varCharVector = new VarCharVector("varchar", allocator);

        //给其填充值
        for(int i=0;i<100000;i++) {
            varCharVector.setSafe(i, ("test" + i).getBytes(StandardCharsets.UTF_8));
        }

        //设置count
        varCharVector.setValueCount(100000);

        //封装进VectorSchemaRoot流水线
        List<Field> fields = Arrays.asList(varCharVector.getField());
        List<FieldVector> vectors = Arrays.asList(varCharVector);
        VectorSchemaRoot root = new VectorSchemaRoot(fields, vectors);

        //写进输出流
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ArrowFileWriter writer = new ArrowFileWriter(root, null, Channels.newChannel(out));
        writer.start();
        //写RecordBatch
        writer.writeBatch();
        writer.end();
        FileOutputStream fileOutputStream = null;
        try{
            fileOutputStream = new FileOutputStream("D:\\rheem\\testRead.txt");
            fileOutputStream.write(out.toByteArray());
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        long start1 = System.nanoTime();
        //inputStream(Paths.get("D:\\rheem\\testRead.txt"));
        //bufferedInputStream(Paths.get("D:\\rheem\\testRead.txt"));
        //randomAccessFile(Paths.get("D:\\rheem\\testRead.txt"));
        mappedFile(Paths.get("D:\\rheem\\testRead.txt"));
        long end1 = System.nanoTime();
        System.out.println("1：" + (end1 - start1) +"ns");

        long startArrow = System.nanoTime();
        try(ArrowFileReader reader = new ArrowFileReader(new ByteArrayReadableSeekableByteChannel(out.toByteArray()), allocator)) {
            reader.loadNextBatch();
            VectorSchemaRoot readBatch = reader.getVectorSchemaRoot();
        } catch (IOException e) {
            e.printStackTrace();
        }
        long endArrow = System.nanoTime();
        System.out.println("A：" + (endArrow - startArrow) +"ns");

        writer.close();
        varCharVector.close();
    }
}

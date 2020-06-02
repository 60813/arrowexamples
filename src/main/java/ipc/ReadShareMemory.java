package ipc;

import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;

//从伪共享内存中读数据
public class ReadShareMemory {

    public static void main(String[] args) throws Exception {
        RandomAccessFile raf = new RandomAccessFile("D:\\rheem\\tmp2.mm",  "rw");
        FileChannel fc = raf.getChannel();
        MappedByteBuffer mbb = fc.map(MapMode.READ_WRITE, 0, 1024);
        int lastIndex = 0;

        for(int i=1;i<27;i++) {
            int flag = mbb.get(0); //读写数据的标志
            int index = mbb.get(1); //读写数据的位置，2为可读
            if(flag!=2 || index == lastIndex) {
                //假如不可读，或未写入新数据时重复循环
                i--;
                continue;
            }
            lastIndex = index;
            System.out.println("程序 ReadShareMemory：" +
                    System.currentTimeMillis() +
                    "：位置L：" + index + " 读出数据：" + (char)mbb.get(index));
            mbb.put(0, (byte)0); //置第一个字节为可读标志为0
            if(index == 27) {
                //读完数据后退出
                break;
            }
        }
    }
}

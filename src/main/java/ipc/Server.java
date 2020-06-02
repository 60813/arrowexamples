package ipc;

//*****************************************************************
// server.java
//
// Allows clients to connect and request files.  If the file
// exists it sends the file to the client.
//*****************************************************************

import java.lang.*;
import java.io.*;
import java.net.*;
import java.nio.Buffer;
import java.util.Scanner;

public class Server {
    private final static int PORT = 12345;
    private final static int QUEUE_SIZE = 10;
    private final static int BUF_SIZE = 4096;

    public static void main(String[] args) {
        try{
            //创建socket
            ServerSocket sSocket = new ServerSocket(PORT, QUEUE_SIZE);

            //socket已搭好，准备等待连接
            while(true) {
                //监听一个连接，有就接收
                Socket cSocket = sSocket.accept();
                byte[] byteArray = new byte[BUF_SIZE];

                //获取从客户端来的文件的名字
                Scanner scn = new Scanner(cSocket.getInputStream());
                String fileName = scn.next();

                //发送文件的内容
                BufferedInputStream bis = new BufferedInputStream(new FileInputStream(fileName));
                OutputStream outStream = cSocket.getOutputStream();
                while(bis.available()>0) {
                    bis.read(byteArray, 0, byteArray.length);
                    outStream.write(byteArray, 0, byteArray.length);
                }

                //Close
                bis.close();
                cSocket.close();
            }
        }
        catch (EOFException eofe) {
            eofe.printStackTrace();
            System.exit(1);
        }
        catch (FileNotFoundException fnfe) {
            fnfe.printStackTrace();
            System.exit(1);
        }
        catch (IllegalArgumentException iae) {
            iae.printStackTrace();
            System.out.println("Bind failed");
            System.exit(1);
        }
        catch(IOException ioe) {
            ioe.printStackTrace();
            System.out.println("Could not complete request");
            System.exit(1);
        }
    }
}

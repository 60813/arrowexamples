package ipc;

//*****************************************************************
// client.java
//
// Connects to the server and sends a request for a file by
// the file name.  Prints the file contents to standard output.
//*****************************************************************

import java.lang.*;
import java.io.*;
import java.net.*;

public class Client {
    private final static int PORT = 12345;
    private final static int BUF_SIZE = 4096;

    public static void main(String [] args) {
        //配置一个socket，使用host name和port number
        try{
            //从命令行参数中获取host name和file name
            String host = args[0];
            String fileName = args[1];
            Socket s = new Socket(host, PORT);
            byte[] byteArray = new byte[BUF_SIZE];
            //发送file name给server
            PrintWriter pw = new PrintWriter(s.getOutputStream(), true);
            pw.println(fileName);

            //从server中获取文件并打印到命令行上
            DataInputStream fromServer = new DataInputStream(s.getInputStream());

            while(fromServer.read(byteArray) > 0) {
                System.out.println(new String(byteArray, "UTF-8"));
            }

            fromServer.close();
            s.close();
        }
        catch (IndexOutOfBoundsException iobe) {
            System.out.println("Usage: client host-name file-name");
            System.exit(1);
        }
        catch (UnknownHostException unhe) {
            unhe.printStackTrace();
            System.out.println("Unknown Host, Socket");
            System.exit(1);
        }
        catch (IOException ioe) {
            ioe.printStackTrace();
            System.exit(1);
        }
    }
}

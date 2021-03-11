import java.net.*;
import java.io.ObjectOutputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.util.concurrent.*;

class Client {

    private static class MyThread extends Thread {
        private String dest;
        private int port;
        private Socket socket;
        private Runnable r;

        public MyThread(Runnable r, String dest, int port) {
            this.dest = dest;
            this.port = port;
            this.r = r;
            System.out.println("this is execd");
            // init the socket and open up the streams
            try {
                this.socket = new Socket(InetAddress.getByName(this.dest), this.port);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        public void run() {
            this.r.run();
        }

        public Socket getSocket() {
            return this.socket;
        }

    }

    private static class MyFactory implements ThreadFactory {
        private String dest;
        private int port;

        public MyFactory(String dest, int port) {
            this.dest = dest;
            this.port = port;
        }

        @Override
        public Thread newThread(Runnable r){
            Thread newt = new MyThread(r, this.dest, this.port);
            return newt;
        }
    }

    private static class Request implements Runnable {
        String string;
        public Request(String str) {
            this.string = str;
        }
        
        @Override
        public void run() {
            try {
                Socket s = ((MyThread)Thread.currentThread()).getSocket();
                ObjectOutputStream out = new ObjectOutputStream(s.getOutputStream());
                BufferedReader in = new BufferedReader(new InputStreamReader(s.getInputStream()));
                out.writeObject(this.string);
                String str = in.readLine();
                System.out.println(str);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    
    public static void main(String[] args) throws IOException{


        ExecutorService exe = Executors.newFixedThreadPool(5, new MyFactory("127.0.0.1", 5678));
        for(int i = 0; i<100; i++) {
            exe.execute(new Request("hello " + i + "\n"));
        }
        exe.shutdown();

        
        // exp 1: one socket, one stream, multiple requests
        // exp3: quick and dirty fix
        // Socket s = new Socket(InetAddress.getByName("127.0.0.1"), 5678);
        
        // for(int i = 0; i< 10; i++) {
        //     ObjectOutputStream out = new ObjectOutputStream(s.getOutputStream());
        //     BufferedReader in = new BufferedReader(new InputStreamReader(s.getInputStream()));
        //     out.writeObject("hello " + i + "\n");
        //     String str;
        //     System.out.println(in.readLine());
        // }
        // s.close();
        // viewed as one packet
        
        /*
        //exp2: 1 socket, multiple streams, one request per stream
        Socket s = new Socket(InetAddress.getByName("127.0.0.1"), 5678);
        for(int i = 0; i< 10; i++) {
            ObjectOutputStream out = new ObjectOutputStream(s.getOutputStream());
            out.writeObject("hello " + i + "\n");
            // out.close();
        }
        s.close();
        // conclusion: probably the same stream
        */

        

    }
}




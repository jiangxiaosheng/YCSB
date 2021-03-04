import java.net.*;
import java.io.BufferedReader;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectOutputStream;

/**
Service class to receive connections and respond to incoming requests.
 */
class Service {
    private static class Response implements Runnable {
        private final Socket sock;
        public Response(Socket s) {
            this.sock = s;
        }

        @Override
        public void run() {
            try {
                BufferedReader in = new BufferedReader(new InputStreamReader(this.sock.getInputStream()));
                ObjectOutputStream out = new ObjectOutputStream(this.sock.getOutputStream());
                String str;
                while((str = in.readLine()) != null) {
                    System.out.println("the string is: " + str);
                    out.writeObject("Pong: " + str + "\n\n");
                }
                System.out.println("out");
                in.close();
                out.close();
            } catch(IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) throws IOException{
        ServerSocket servSock = new ServerSocket(5678);
        ExecutorService exe = Executors.newFixedThreadPool(5);
        while(true) {
            exe.execute(new Response(servSock.accept())); 
        }
        //exe.shutdown();
    }
}
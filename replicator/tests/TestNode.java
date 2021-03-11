import site.ycsb.*;
import java.io.*;
import java.net.*;
import com.google.gson.*;

class TestNode {
	public static void main(String[] args) {
		if (args.length < 3) {
			System.out.println("usage: ");
			System.out.println("java <dependencies> TestNode dest_ip dest_port listen_port");
			return;
		}
	  String dest = args[0];
		int dest_port = Integer.parseInt(args[1]);	
		int l_port = Integer.parseInt(args[2]);
	
		ReplicatorOp op = new ReplicatorOp("ycsb", "world", new byte[5], "insert");
		
		try {
			Socket s = new Socket(InetAddress.getByName(dest), dest_port);
			ObjectOutputStream out = new ObjectOutputStream(s.getOutputStream());
			ServerSocket serv = new ServerSocket(l_port);
			Listen listen = new Listen(serv);
			Thread t = new Thread(listen);
			t.start();
			Gson gson = new Gson();
			out.writeObject(gson.toJson(op) + "\n");
			out.close();
			s.close();
			Thread.sleep(10000);
		  listen.terminate();
			t.join();
			serv.close();					
		} catch (InterruptedException | IOException e) {
			e.printStackTrace();
		}
	
	}	
  
  private static class Listen implements Runnable {
		private ServerSocket serv;
		private boolean isAlive;
		public Listen(ServerSocket serv) {
			this.serv = serv;
			this.isAlive = true;
		}
		public void terminate() { this.isAlive = false; }
		@Override
		public void run(){
			while(isAlive) {
				try {
					Socket soc = this.serv.accept();
					BufferedReader in = new BufferedReader(new InputStreamReader(soc.getInputStream()));
					String str;
					while((str = in.readLine()) != null) {
						System.out.println("recved str: " + str);
					}
					in.close();
					soc.close();
				} catch (IOException e) {
					e.printStackTrace();
				}	
			}
		}
	}	
	
}

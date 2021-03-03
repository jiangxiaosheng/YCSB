import site.ycsb.*;
import java.io.*;
import java.net.*;
import com.google.gson.*;

class TestRep {
	public static void main(String[] args) {
		if (args.length < 2) {
			System.out.println("usage: ");
			System.out.println("java <dependencies> TestRep dest_ip dest_port");
			return;
		}
	  String dest = args[0];
		int dest_port = Integer.parseInt(args[1]);	
	
		ReplicatorOp op = new ReplicatorOp("ycsb", "world", new byte[5], "insert");
		
		try {
			Socket s = new Socket(InetAddress.getByName(dest), dest_port);
			ObjectOutputStream out = new ObjectOutputStream(s.getOutputStream());
			BufferedReader in = new BufferedReader(new InputStreamReader(s.getInputStream()));
			Gson gson = new Gson();
			out.writeObject(gson.toJson(op) + "\n");
			// out.close();
			String str;
//			Thread.sleep(5000);
			System.out.println("cp1");
			while((str = in.readLine()) != null) {
				System.out.println("recved str: " + str);
			}
			System.out.println("cp2");
			in.close();
			s.close();
		} catch (Exception e) {
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
			}
		}
	}	
	
}

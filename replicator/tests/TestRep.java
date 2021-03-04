import site.ycsb.*;
import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.List;
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
		
		List<ReplicatorOp> ops = new ArrayList<>();	
		byte[] inputs = { 0,0,0,6,102,105,101,108,100,49,0,0,0,0,0,0,0,6,102,105,101,108,100,48,0,0,0,0,0,0,0,6,102,105,101,108,100,55,0,0,0,0,0,0,0,6,102,105,101,108,100,54,0,0,0,0,0,0,0,6,102,105,101,108,100,57,0,0,0,0,0,0,0,6,102,105,101,108,100,56,0,0,0,0,0,0,0,6,102,105,101,108,100,51,0,0,0,0,0,0,0,6,102,105,101,108,100,50,0,0,0,0,0,0,0,6,102,105,101,108,100,53,0,0,0,0,0,0,0,6,102,105,101,108,100,52,0,0,0,0 };
    	ops.add(new ReplicatorOp("ycsb", "world", inputs, "insert"));
		ops.add(new ReplicatorOp("ycsb", "world", null, "read"));
		ops.add(new ReplicatorOp("ycsb", "world", inputs, "update"));
		ops.add(new ReplicatorOp("ycsb", "world", null, "delete"));
		
		try {
			Gson gson = new Gson();
			for(int i = 0; i < 100; i++) {
				for(ReplicatorOp op: ops) {
					Socket s = new Socket(InetAddress.getByName(dest), dest_port);
					ObjectOutputStream out = new ObjectOutputStream(s.getOutputStream());
					BufferedReader in = new BufferedReader(new InputStreamReader(s.getInputStream()));
					out.writeObject(gson.toJson(op) + "\n");
					String str;
					while((str = in.readLine()) != null) {
						System.out.println("recved str: " + str);
					}
					in.close();
				out.close();
				s.close();
				}
			}
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

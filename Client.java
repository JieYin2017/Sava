import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.sql.Timestamp;
import java.util.List;
import java.util.Scanner;


public class Client {
	
	private final int PREGEL_PORT = 9000;
	private ResultReceiveThread resultReceiveThread;
	
	private class ResultReceiveThread extends Thread{
		@Override
		public void run(){
			ServerSocket server;
			try {
				server = new ServerSocket(PREGEL_PORT);
				while(true){
					Socket socket = server.accept();
					System.out.println(new Timestamp(System.currentTimeMillis()).toString() + "\t Results received:");
					ObjectInputStream socket_in = new ObjectInputStream(socket.getInputStream());
					Message message = (Message)socket_in.readObject();
					List<Vertex> results = message.getData();
					for(Vertex vertex:results){
						System.out.println(vertex.getVertex_id() + "\t" + vertex.getValue());
					}
				}
				
			} catch (IOException | ClassNotFoundException e) {
				e.printStackTrace();
			}
		}
	}
	
	public static void main(String[] args) {

		PageRankVertex pageRankVertex;
		SSSPVertex ssspVertex;
		
		Client client = new Client();
		client.run();

	}

	private void run() {
		
		resultReceiveThread = new ResultReceiveThread();
		resultReceiveThread.start();
		
		Scanner keyboard = new Scanner(System.in);
		while(true){
			String[] command = keyboard.nextLine().split(" ");
			if(command[0].equals("PageRank") || command[0].equals("SSSP")){
				Socket socket;
				ObjectOutputStream socket_out;
				try {
					socket = new Socket("fa17-cs425-g15-01.cs.illinois.edu", PREGEL_PORT);
					socket_out = new ObjectOutputStream(socket.getOutputStream());
					socket_out.writeObject(new Message("Start a task", command[0], command[1]));
					socket_out.flush();
					socket.close();
					
					socket = new Socket("fa17-cs425-g15-02.cs.illinois.edu", PREGEL_PORT);
					socket_out = new ObjectOutputStream(socket.getOutputStream());
					socket_out.writeObject(new Message("Start a task", command[0], command[1]));
					socket_out.flush();
					socket.close();
				} catch (IOException e) {
					e.printStackTrace();
				}

				System.out.println(new Timestamp(System.currentTimeMillis()).toString() + "\t Task started");
				System.out.println("waiting for results...");
			}
			else{
				System.out.println("App name error! Type again:");
			}
		}
	}

}

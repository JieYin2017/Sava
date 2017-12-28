import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.io.Reader;
import java.net.ConnectException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Scanner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentHashMap.KeySetView;

/*
 * MP2 - Distributed Group Membership
 */

public class MP4 {

	/*
	 * Node in the membership list and messages
	 */
	private class Node {
		private String hostName;
		private String timestamp;
		private int hashID;

		/*
		 * Constructor of Class Node
		 */
		public Node() {
			this.timestamp = getTimestamp();
			this.hostName = getHostName();

			this.hashID = getHashID(new String(this.hostName + this.timestamp));
			while (membershipList[hashID] != null) {
				hashID = (hashID + 1) % 128;
			}
		}
	}

	private class FileRecord {
		private String filename;
		private String addr;
		private long timestamp;

		public FileRecord(String filename, String addr, long timestamp) {
			this.filename = filename;
			this.addr = addr;
			this.timestamp = timestamp;
		}
	}

	/*
	 * Thread for heartbeating
	 */
	private class HeartBeatThread implements Runnable {

		@Override
		public void run() {
			try {
				while (!goingToLeave) {
					synchronized (successors) {
						for (Node node : successors) {
							sendMessage(node.hostName, HEARTBEAT, thisNode);
						}
					}

					Thread.currentThread().sleep(600);
				}
			} catch (InterruptedException e) {

			} catch (UnknownHostException e) {
				// e.printStackTrace();
				// System.out.println("HEARTBEAT target unknown: " +
				// node.hostName);
			}
		}

	}

	/*
	 * Thread for handling the received messages
	 */
	private class MessageHandlerThread implements Runnable {

		@Override
		public void run() {
			byte[] buf = new byte[1024];

			try {
				DatagramSocket ds = new DatagramSocket(SERVER_PORT);
				DatagramPacket dp_receive = new DatagramPacket(buf, 1024);
				while (!goingToLeave) {
					ds.receive(dp_receive);
					String[] message = new String(dp_receive.getData()).split("\n");
					int messageType = Integer.parseInt(message[0]);
					Node node = new Node();
					if (messageType != DELETEFILE && messageType != NEWFILERECORD) {
						node.hostName = message[1];
						node.timestamp = message[2];
						node.hashID = Integer.parseInt(message[3]);
					}

					// System.out.println("New Message Received! " +
					// messageType);
					switch (messageType) {
					case HEARTBEAT:
						updateHBRecord(node);
						break;
					case JOIN:
						newGroupMember(node);
						break;
					case MEMBERSHIPLIST:
						initMembershipList(node);
						break;
					case NEWMEMBER:
						addMember(node);
						break;
					case LEAVE:
						memberLeave(node);
						break;
					case FAILURE:
						markAsFailure(node.hashID);
						break;
					case NEWFILERECORD:
						addFileRecord(message[1], message[2], Long.parseLong(message[3]));
						break;
					case DELETEFILE:
						deleteFile(message[1]);
						break;
					default:
						break;
					}
				}
				ds.close();
			} catch (SocketException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

	}

	/*
	 * Thread for failure detection
	 */
	private class FailureDetectorThread implements Runnable {

		@Override
		public void run() {

			try {
				while (!goingToLeave) {
					synchronized (heartbeatCnt) {
						for (Integer key : heartbeatCnt.keySet()) {
							long curTime = System.currentTimeMillis();
							if (curTime - heartbeatCnt.get(key) > 2000) {
								markAsFailure(key);
							}
						}

					}
					Thread.currentThread().sleep(1000);

				}
			} catch (InterruptedException e) {

			}
		}

	}

	private class FileTransferThread extends Thread {
		private ServerSocket server = null;

		@Override
		public void interrupt() {
			try {
				server.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		@Override
		public void run() {

			while (!goingToLeave) {
				try {
					server = new ServerSocket(SERVER_PORT);
					while (!goingToLeave) {
						Socket socket = server.accept();
						PrintWriter socket_out = new PrintWriter(socket.getOutputStream(), true);
						BufferedReader socket_in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
						String request = socket_in.readLine();
						System.out.println("Received a request: " + request);
						switch (request) {
						case "Get":
							responseToGet(socket, socket_in, socket_out);
							break;
						case "Put":
							responseToPut(socket, socket_in, socket_out);
							break;
						case "Replica":
							responseToReplica(socket, socket_in, socket_out);
							break;
						default:
							break;
						}

						socket_in.close();
						socket_out.close();
						socket.close();
					}

				} catch (IOException e) {
					try {
						server.close();
					} catch (IOException e1) {
						e1.printStackTrace();
					}
					e.printStackTrace();
				}
			}
		}
	}

	private class SendReplicaThread extends Thread {

		@Override
		public void run() {
			KeySetView<String, List<FileRecord>> filenames;
			synchronized (fileRecords) {
				filenames = fileRecords.keySet();
			}
			for (String filename : filenames) {
				boolean hasThisFile = false;
				for (FileRecord fileRecord : fileRecords.get(filename)) {
					if (fileRecord.addr.equals(thisNode.hostName)) {
						hasThisFile = true;
						break;
					}
				}
				if (!hasThisFile)
					continue;
				String[] addresses = getReplicasAddr(filename);
				for (String addr : addresses) {
					if (!addr.equals(thisNode.hostName)) {
						try {
							Socket socket = new Socket(addr, SERVER_PORT);
							PrintWriter socket_out = new PrintWriter(socket.getOutputStream(), true);
							BufferedReader socket_in = new BufferedReader(
									new InputStreamReader(socket.getInputStream()));
							System.out.println("Sending replica request to " + addr);
							socket_out.println("Replica");
							socket_out.println(filename);
							socket_out.flush();
							if (socket_in.readLine().equals("Cancel")) {
								System.out.println("Canceled!");
								socket_in.close();
								socket_out.close();
								socket.close();
								continue;
							}
							socket_out.println(fileRecords.get(filename).get(0).timestamp);
							socket_out.flush();

							BufferedInputStream file_in_s = new BufferedInputStream(new FileInputStream(filename));
							BufferedOutputStream socket_out_s = new BufferedOutputStream(socket.getOutputStream());
							System.out.println("Sending replica...");
							byte[] line = new byte[4096];
							int len;
							while ((len = file_in_s.read(line)) != -1) {
								socket_out_s.write(line, 0, len);
							}
							System.out.println("Sending replica complete");
							socket_out_s.flush();
							file_in_s.close();
							socket_out_s.close();
							socket_out.close();

							socket.close();
						} catch (UnknownHostException e) {
							e.printStackTrace();
						} catch (ConnectException e) {
							// TODO: handle exception
							return;
						} catch (IOException e) {
							e.printStackTrace();
						}

					}
				}

			}

		}

	}

	private class PregelMessageThread extends Thread {

		private int numWorkers;
		private int reportCnt;
		private int notChangedCnt;
		private List<String> workerIDList;
		private int numResultsReceived;
		private List<PageRankVertex> PRresults;
		private List<SSSPVertex> SSSPresults;
		Message masterMessage;

		@Override
		public void run() {

			try {
				ServerSocket server = new ServerSocket(PREGEL_PORT);

				while (true) {
					try {
						Socket socket = server.accept();
						ObjectInputStream sin = new ObjectInputStream(socket.getInputStream());
						Message message = (Message) sin.readObject();
						if (!(getHostName().equals("fa17-cs425-g15-01.cs.illinois.edu")
								|| getHostName().equals("fa17-cs425-g15-02.cs.illinois.edu"))) {
							if (!message.getMessageType().equals("Start a task") && workerThread == null
									&& !message.getMessageType().equals("Start workers"))
								continue;
						}
						switch (message.getMessageType()) {
						case "Start a task":
							masterMessage = message;
							masterMessage.setMessageType("Start workers");
							masterMessage.setMasterHostname(getHostName());
							if (getHostName().equals("fa17-cs425-g15-02.cs.illinois.edu"))
								break;
							startTask();
							break;
						case "Start workers":
							startWorkers(message);
							break;
						case "workerReport":
							workerReport(message);
							break;
						case "nextRound":
							nextRound(message);
							break;
						case "neighborMessage":
							neighborMessage(message);
							break;
						case "result request":
							dataToMaster();
							break;
						case "results":
							mergeResults(message);
							break;
						default:
							break;
						}

						socket.close();
					} catch (Exception e) {
						// TODO: handle exception
					}
				}
			} catch (IOException e) {
				e.printStackTrace();
			}

		}

		private void mergeResults(Message message) {
			System.out.println("===============results from worker received"
					+ new Timestamp(System.currentTimeMillis()).toString());

			if (masterMessage.getApp().equals("PageRank"))
				PRresults.addAll(message.getData());
			else
				SSSPresults.addAll(message.getData());

			numResultsReceived++;
			if (numResultsReceived == numWorkers) {

				System.out.println("=========All results received");
				outputToClient();
			}
		}

		private void outputToClient() {

			Collections.sort(SSSPresults);
			Collections.sort(PRresults);
			try {
				FileOutputStream fout = new FileOutputStream(new File("result.txt"));
				PrintWriter dout = new PrintWriter(fout);
				for (Vertex vertex : masterMessage.getApp().equals("PageRank") ? PRresults : SSSPresults) {
					dout.println(vertex.id + "\t" + vertex.value);
				}
				fout.close();
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
			if (masterMessage.getApp().equals("PageRank")) {

				sendMessageTo("fa17-cs425-g15-10.cs.illinois.edu",
						new Message("final results", new ArrayList<PageRankVertex>(PRresults.subList(0, 25))));
			} else {

				sendMessageTo("fa17-cs425-g15-10.cs.illinois.edu", new Message("final results", SSSPresults));

			}
		}

		private void dataToMaster() {
			((WorkerThread) workerThread).setStop(true);
			workerThread.interrupt();
			workerThread = null;

			// try {
			// FileOutputStream fout = new FileOutputStream(new
			// File("result.txt"));
			// PrintWriter dout = new PrintWriter(fout);
			// for (Vertex vertex : vertexHashMap.values()) {
			// dout.println(vertex.id + "\t" + vertex.value);
			// }
			// fout.close();
			// } catch (FileNotFoundException e) {
			// e.printStackTrace();
			// } catch (IOException e) {
			// e.printStackTrace();
			// }

			List<Vertex> data = new ArrayList<Vertex>();
			for (Vertex vertex : vertexHashMap.values()) {
				vertex.clearMessageList();
				vertex.clearOutEdgeList();
				data.add(vertex);
			}
			System.out.println("==========sending results to the master");
			sendMessageTo(masterMessage.getMasterHostname(), new Message("results", data));

		}

		public void neighborMessage(Message message) {
			// if (vertexHashMap.containsKey(message.getTargetVertexID())) {
			// Vertex vertex = vertexHashMap.get(message.getTargetVertexID());
			// vertex.addMessage(message);
			// } else {
			//
			// Vertex vertex = null;
			//
			//
			// if (masterMessage.getApp().equals("PageRank"))
			// vertex = new PageRankVertex(message.getTargetVertexID(), 1.0 /
			// Vertex.numVertices);
			//
			// else if (masterMessage.getApp().equals("SSSP")) {
			// // !!! vertex = new SSSPVertex(message.getTargetVertexID(),
			// // 0.0);
			// }
			// vertex.supersteps = message.getSuperstep() - 1;
			// vertex.addMessage(message);
			// vertexHashMap.put(message.getTargetVertexID(), vertex);
			// }
			List<Message> list = message.getData();
			for (Message m : list) {
				Vertex vertex = vertexHashMap.get(m.getTargetVertexID());
				if (vertex != null)
					vertex.addMessage(m);
			}

		}

		public void nextRound(Message message) {
			System.out.println("==================receive next round command");
			workerThread.interrupt();

		}

		public void workerReport(Message message) {

			System.out.println("==============worker report received");
			reportCnt++;
			if (!message.getChangedStatus())
				notChangedCnt++;
			if (reportCnt == numWorkers) {
				if (numWorkers == notChangedCnt) {
					System.out.println("=====================task accomplished!!!");
					for (String dest : workerIDList) {
						sendMessageTo(dest, new Message("result request"));
					}
					return;
				}
				reportCnt = 0;
				notChangedCnt = 0;
				System.out.println("====================asking workers to do the next round");
				for (String dest : workerIDList) {
					sendMessageTo(dest, new Message("nextRound"));
				}
			}
		}

		public void startWorkers(Message message) {

			System.out.println("==========startWorker received!");
			if (workerThread != null) {
				System.out.println("workerThread  not null");
				((WorkerThread) workerThread).setStop(true);
				workerThread.interrupt();
			}
			masterMessage = message;
			workerThread = new WorkerThread(message);

			try {
				Thread.currentThread().sleep(500);
			} catch (InterruptedException e1) {
				e1.printStackTrace();
			}
			vertexHashMap.clear();
			workerThread.start();

		}

		public void startTask() {
			numWorkers = 0;
			reportCnt = 0;
			notChangedCnt = 0;
			numResultsReceived = 0;
			workerIDList = new ArrayList<String>();
			PRresults = new ArrayList<PageRankVertex>();
			SSSPresults = new ArrayList<SSSPVertex>();

			if (getName().equals("")) {
				try {
					Thread.currentThread().sleep(1000);
				} catch (InterruptedException e1) {
					e1.printStackTrace();
				}
			}

			synchronized (membershipList) {
				for (Node node : membershipList) {
					if (node != null) {
						if (!(node.hostName.equals("fa17-cs425-g15-01.cs.illinois.edu")
								|| node.hostName.equals("fa17-cs425-g15-02.cs.illinois.edu"))) {
							workerIDList.add(node.hostName);
							numWorkers++;

						}
					}
				}
			}

			getFile(masterMessage.getFilename(), "Input" + masterMessage.getFilename());
			HashSet<Integer> IDSet = new HashSet<>();
			try {
				Scanner fout = new Scanner(new File("Input" + masterMessage.getFilename()));
				while (fout.hasNextInt()) {
					int id = fout.nextInt();
					if (!IDSet.contains(id))
						IDSet.add(id);
				}
				fout.close();
				masterMessage.setNumVertices(IDSet.size());
				System.out.println("Vertex num: " + IDSet.size());
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}

			System.out.println("========sending start worker command");
			for (String dest : workerIDList) {
				sendMessageTo(dest, masterMessage);
			}
			System.out.println("========sending start worker command compelete");

		}

	}

	private class WorkerThread extends Thread {
		private boolean stop;
		private Message message;
		private List<String> workerIDList;
		private int numWorker;
		private boolean changed;
		private List<LinkedList<Message>> messageBuffer;

		public WorkerThread(Message message) {
			super();
			this.message = message;
			stop = false;
			changed = false;
			workerIDList = new ArrayList<String>();
			numWorker = 0;
			messageBuffer = new ArrayList<LinkedList<Message>>();
			System.out.println("============starting to initialize message buffer");
			for (int i = 0; i < 7; i++) {
				LinkedList<Message> list = new LinkedList<Message>();
				messageBuffer.add(list);
			}

		}

		public void setStop(boolean stop) {
			this.stop = stop;
		}

		@Override
		public void run() {

			System.out.println("=========Worker started!!!");

			getFile(message.getFilename(), "Input" + message.getFilename());
			if (stop)
				return;
			createWorkerID();
			readFile("Input" + message.getFilename());
			if (stop)
				return;

			System.out.println(
					"=============Loading Complete!!!! " + new Timestamp(System.currentTimeMillis()).toString());
			while (!stop) {

				for (int i = 0; i < 7; i++) {
					messageBuffer.get(i).clear();
				}

				synchronized (vertexHashMap) {

					for (Vertex vertex : vertexHashMap.values()) {

						if (vertex.compute(workerIDList, messageBuffer))
							changed = true;
						if (stop)
							return;
					}
				}

				System.out.println("===========computation compelete!!!");

				for (int i = 0; i < 7; i++) {
					LinkedList<Message> list = messageBuffer.get(i);
					if (list.size() != 0)
						sendMessageTo(workerIDList.get(i), new Message("neighborMessage", list));
				}

				System.out.println("========sending report to master");

				sendMessageTo(message.getMasterHostname(), new Message("workerReport", changed));
				changed = false;

				try {
					Thread.currentThread().sleep(Long.MAX_VALUE);
				} catch (InterruptedException e) {
					if (stop)
						return;
				}
			}

		}

		public void createWorkerID() {
			synchronized (membershipList) {
				for (Node node : membershipList) {
					if (node != null) {
						if (!(node.hostName.equals("fa17-cs425-g15-01.cs.illinois.edu")
								|| node.hostName.equals("fa17-cs425-g15-02.cs.illinois.edu"))) {
							workerIDList.add(node.hostName);
							numWorker++;

						}
					}
				}
			}
		}

		// read input data into vertex hashmap
		public void readFile(String filename) {

			System.out.println("========Worker starting reading file");
			try {
				Vertex.numVertices = message.getNumVertices();
				Scanner fout = new Scanner(new File(filename));
				while (fout.hasNextInt()) {
					int vertexID = fout.nextInt();
					int nextID = fout.nextInt();
					if (workerIDList.get(vertexID % numWorker).equals(getHostName())) {
						if (!vertexHashMap.containsKey(vertexID)) {

							Vertex vertex = null;

							if (message.getApp().equals("PageRank")) {
								vertex = new PageRankVertex(vertexID, 1.0);
								vertex.addOutEdge(new Edge(nextID));
							} else if (message.getApp().equals("SSSP")) {
								vertex = new SSSPVertex(vertexID, Integer.MAX_VALUE);
								vertex.addOutEdge(new Edge(nextID, 1));
							}

							vertexHashMap.put(vertexID, vertex);
						} else {
							Vertex vertex = vertexHashMap.get(vertexID);

							if (message.getApp().equals("PageRank"))
								vertex.addOutEdge(new Edge(nextID));
							else if (message.getApp().equals("SSSP")) {
								vertex.addOutEdge(new Edge(nextID, 1));
							}
						}

					}
					if (workerIDList.get(nextID % numWorker).equals(getHostName())) {
						if (!vertexHashMap.containsKey(nextID)) {

							Vertex vertex = null;

							if (message.getApp().equals("PageRank"))
								vertex = new PageRankVertex(nextID, 1.0);
							else if (message.getApp().equals("SSSP")) {
								vertex = new SSSPVertex(nextID, Integer.MAX_VALUE);
							}
							vertexHashMap.put(nextID, vertex);
						}
					}

					if (stop)
						return;
				}
				fout.close();
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}

			System.out.println("========Worker reading file complete");
		}

	}

	/*
	 * Constructor of Class DGM
	 */
	public MP4() {
		// implemented in the initializeDGM method
	}

	// send pregel message
	public void sendMessageTo(String hostName, Message message) {
		try {
			Socket socket = new Socket(hostName, PREGEL_PORT);
			ObjectOutput sout = new ObjectOutputStream(socket.getOutputStream());
			sout.writeObject(message);
			sout.flush();
			sout.close();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	// message type
	private final int HEARTBEAT = 0;
	private final int JOIN = 1;
	private final int MEMBERSHIPLIST = 2;
	private final int NEWMEMBER = 3;
	private final int LEAVE = 4;
	private final int FAILURE = 5;
	private final int NEWFILERECORD = 6;
	private final int DELETEFILE = 7;

	private final int CLIENT_PORT = 8300;
	private final int SERVER_PORT = 8399;
	private final int PREGEL_PORT = 9000;

	// member-variables
	private boolean is_introducer;

	boolean masterDead = false;
	private Node[] membershipList;
	private Node thisNode;
	private List<Node> successors;
	private List<Node> predecessors;
	private ConcurrentHashMap<String, List<FileRecord>> fileRecords;

	private boolean in_group;
	private boolean goingToLeave;

	private Thread heartBeatThread;
	private Thread messageHandlerThread;
	private Thread failureDetectorThread;
	private Thread fileTransferThread;

	private Thread workerThread;
	private Thread pregelMessageThread;
	private ConcurrentHashMap<Integer, Long> heartbeatCnt;
	private ConcurrentHashMap<Integer, Vertex> vertexHashMap;

	public static void main(String[] args) {

		MP4 mp4 = new MP4();
		mp4.start();
	}

	/*
	 * Starts to take commands from users
	 */
	private void start() {
		Scanner keyboard = new Scanner(System.in);
		String[] command;
		while (true) {
			System.out.print("Your command(join, leave, showlist, showid, ls, store, put, get, detele):");
			command = keyboard.nextLine().split(" ");

			switch (command[0]) {
			case "join":
				joinGroup();
				break;
			case "leave":
				leaveGroup();
				break;
			case "showlist":
				showList();
				break;
			case "showid":
				showID();
				break;
			case "ls":
				if (command.length != 2) {
					System.out.println("Unsupported command");
					break;
				}
				showFileReplica(command[1]);
				break;
			case "store":
				showFiles();
				break;
			case "put":
				if (command.length != 3) {
					System.out.println("Unsupported command");
					break;
				}
				putFile(command[1], command[2], keyboard);
				break;
			case "get":
				if (command.length != 3) {
					System.out.println("Unsupported command");
					break;
				}
				getFile(command[1], command[2]);
				break;
			case "delete":
				if (command.length != 2) {
					System.out.println("Unsupported command");
					break;
				}
				sendDelete(command[1]);
				break;
			default:
				System.out.println("Unsupported Command!");
				break;
			}
		}

	}

	private void sendDelete(String sdfsfilename) {
		String[] addresses = getReplicasAddr(sdfsfilename);
		String address = addresses[0];
		if (address == null) {
			System.out.println("Error!!" + sdfsfilename + " not stored on the cluster");
			return;
		}
		synchronized (membershipList) {
			for (Node node : membershipList) {
				if (node != null) {
					try {
						sendMessage(node.hostName, DELETEFILE, sdfsfilename);
					} catch (UnknownHostException e) {
						e.printStackTrace();
					}
				}
			}
		}

	}

	/*
	 * Get a file from remote
	 */
	private void getFile(String sdfsfilename, String localfilename) {
		String[] addresses = getReplicasAddr(sdfsfilename);

		if (addresses == null || addresses[0] == null) {
			System.out.println("Error!!" + sdfsfilename + " not stored on the cluster");
			return;
		}
		String address = addresses[new Random().nextInt(2)];
		Socket socket;
		try {
			socket = new Socket(address, SERVER_PORT);
			PrintWriter socket_out = new PrintWriter(socket.getOutputStream(), true);

			// send get request
			System.out.println("sending get request");
			socket_out.println("Get");
			socket_out.println(sdfsfilename);

			BufferedReader socket_in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
			if (socket_in.readLine().equals("Not Found")) {
				System.out.println("Error!!" + sdfsfilename + " not stored on the cluster");
				socket_in.close();
				socket_out.close();
				socket.close();
				return;
			}

			// receive file data
			System.out.println("Starting receiving " + sdfsfilename + " from " + address + " ...");
			BufferedOutputStream file_out_s = new BufferedOutputStream(new FileOutputStream(localfilename));
			BufferedInputStream socket_in_s = new BufferedInputStream(socket.getInputStream());
			byte[] line = new byte[4096];
			int len;
			while ((len = socket_in_s.read(line)) != -1) {
				file_out_s.write(line, 0, len);
			}

			file_out_s.flush();
			file_out_s.close();
			socket_in.close();
			socket_out.close();
			socket_in_s.close();
			socket.close();
			System.out.println(sdfsfilename + " received");
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	/*
	 * Upload a file to SDFS from local dir
	 */
	private void putFile(String localfilename, String sdfsfilename, Scanner keyboard) {
		long timestamp = System.currentTimeMillis();
		String[] addresses = getReplicasAddr(sdfsfilename);
		try {
			Boolean conti = null;
			for (String address : addresses) {
				if (address != null) {

					Socket socket = new Socket(address, SERVER_PORT);
					PrintWriter socket_out = new PrintWriter(socket.getOutputStream(), true);
					BufferedInputStream file_in_s = new BufferedInputStream(new FileInputStream(localfilename));
					BufferedReader socket_in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
					// sending command, sdfsfilename and timestamp
					System.out.println("sending put request to " + address);
					socket_out.println("Put");
					socket_out.flush();
					socket_out.println(sdfsfilename);
					socket_out.println(timestamp);
					socket_out.flush();
					String status = socket_in.readLine();
					if (status.equals("Confirmation")) {
						if (conti == null) {
							System.out.print("Less than 1 minute gap between consecutive updates! Continue?(y/n):");
							long timestampforconfir = System.currentTimeMillis();
							String confirmation = keyboard.nextLine();
							while (!(confirmation.equals("y") || confirmation.equals("n"))) {
								System.out.print("Continue?(y/n):");
								confirmation = keyboard.nextLine();
							}
							if (confirmation.equals("n")
									|| (System.currentTimeMillis() - timestampforconfir) > 30 * 1000) {
								System.out.println();
								System.out.println("Update canceled!");
								socket_out.println("Cancel");
								file_in_s.close();
								socket_out.close();
								socket_in.close();
								socket.close();
								return;
							} else {
								conti = new Boolean(true);
								socket_out.println("Continue");
							}
						} else {
							socket_out.println("Continue");
						}
					}

					System.out.println("sending file...");
					BufferedOutputStream socket_out_s = new BufferedOutputStream(socket.getOutputStream());
					byte[] line = new byte[4096];
					int len;
					while ((len = file_in_s.read(line)) != -1) {
						socket_out_s.write(line, 0, len);
					}
					System.out.println("sending compeleted");
					socket_out_s.flush();
					file_in_s.close();
					socket_out.close();
					socket_out_s.close();
					socket_in.close();
					socket.close();
				}
			}
		} catch (FileNotFoundException e) {
			System.out.println("Error!!Can not find file: " + localfilename);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 
	 * @param sdfsfilename
	 * @return an array of the addresses of the replicas
	 */
	private String[] getReplicasAddr(String sdfsfilename) {
		int fileHashID = getHashID(sdfsfilename);
		System.out.println("getReplicasAddr, filename:" + sdfsfilename);
		List<String> list = new LinkedList<>();
		int cnt = 0;
		for (int i = fileHashID;;) {
			if (membershipList[i] != null) {
				list.add(membershipList[i].hostName);
				cnt++;
			}
			i = i == 127 ? 0 : i + 1;
			if (i == fileHashID || cnt == 3)
				break;
		}

		String[] addresses = new String[list.size()];
		for (int i = 0; i < list.size(); i++) {
			addresses[i] = list.get(i);
		}

		return addresses;
	}

	/*
	 * list all machine (VM) addresses where this file is currently being stored
	 */
	private void showFileReplica(String sdfsfilename) {
		synchronized (fileRecords) {
			if (fileRecords.containsKey(sdfsfilename)) {
				List<FileRecord> records = fileRecords.get(sdfsfilename);
				System.out.println(sdfsfilename + " is currently stored on:");
				for (FileRecord record : records) {
					System.out.println(record.addr);
				}

			} else {
				System.out.println("Error!!" + sdfsfilename + " not stored on the cluster");
			}
		}

	}

	/*
	 * list all files currently being stored at this machine
	 */
	private void showFiles() {
		System.out.println("Files stored at this machine:");
		synchronized (fileRecords) {
			for (String sdfsfilename : fileRecords.keySet()) {
				List<FileRecord> records = fileRecords.get(sdfsfilename);
				for (FileRecord record : records) {
					if (record.addr.equals(thisNode.hostName)) {
						System.out.println(sdfsfilename);
						break;
					}
				}
			}
		}

	}

	/*
	 * Show the ID of the node(HostName and its time stamp)
	 */
	private void showID() {
		if (in_group)
			System.out.println("ID: " + thisNode.hostName + " " + thisNode.timestamp);
		else
			System.out.println("Please join the group first!");
	}

	/*
	 * Show the membership list of the node
	 */
	private void showList() {
		if (in_group) {
			System.out.println("Membership list of " + thisNode.hostName + "=======");
			synchronized (failureDetectorThread) {
				for (Node node : membershipList) {
					if (node != null)
						System.out.println(node.hostName + " " + node.timestamp);
				}
			}

		} else
			System.out.println("Please join the group first!");
	}

	/*
	 * Leave the group and send the leave message to its successors
	 */
	private void leaveGroup() {
		if (!in_group) {
			System.out.println("You haven't joined a group!");
			return;
		}

		// DatagramSocket ds = new DatagramSocket(SERVER_PORT);
		// Send the leave message to the successors
		synchronized (successors) {
			for (Node successor : successors) {
				try {
					sendMessage(successor.hostName, LEAVE, thisNode);
				} catch (UnknownHostException e) {
					e.printStackTrace();
					System.out.println("Can't find the destination host: " + successor.hostName);
				}

			}
		}
		nodeReset();
	}

	/*
	 * Send a request to the introducer
	 */
	private void joinGroup() {
		if (in_group) {
			System.out.println("This node is already in the group!");
			return;
		}

		if (heartBeatThread == null) {
			initializeDGM();
			heartBeatThread.start();
			messageHandlerThread.start();
			failureDetectorThread.start();
			fileTransferThread.start();
			pregelMessageThread.start();
		}

		if (!is_introducer) {
			try {
				sendMessage("fa17-cs425-g15-01.cs.illinois.edu", JOIN, thisNode);
			} catch (UnknownHostException e) {
				e.printStackTrace();
				// System.out.println("The introducer is down! Can't join the
				// group!");
			}
		} else {
			if (!searchGroup()) {
				System.out.println("I'm the introducer. Group created!");
				in_group = true;
			} else
				System.out.println("Rejoin the group");
		}
	}

	/*
	 * Search the existing group in the network
	 */
	private boolean searchGroup() {
		String s1 = "fa17-cs425-g15-";
		String s2 = ".cs.illinois.edu";
		for (int i = 2; i <= 10; i++) {
			try {
				if (i < 10)
					sendMessage(s1 + "0" + i + s2, JOIN, thisNode);
				else
					sendMessage(s1 + i + s2, JOIN, thisNode);
			} catch (UnknownHostException e) {
				e.printStackTrace();
			}
			try {
				Thread.currentThread().sleep(200);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			if (in_group) {
				return true;
			}
		}
		return false;
	}

	/*
	 * Initialize member variables of this node
	 * 
	 * @param string: host name
	 */
	private void initializeDGM() {
		// Judge if this vm is the introducer
		if (getHostName().equals("fa17-cs425-g15-01.cs.illinois.edu"))
			is_introducer = true;
		else
			is_introducer = false;

		membershipList = new Node[128];
		thisNode = new Node();
		successors = new ArrayList<>();
		predecessors = new ArrayList<>();
		heartbeatCnt = new ConcurrentHashMap<>();
		fileRecords = new ConcurrentHashMap<>();
		// numOfMembers = 1;
		// in_group = true;
		goingToLeave = false;
		heartBeatThread = new Thread(new HeartBeatThread());
		messageHandlerThread = new Thread(new MessageHandlerThread());
		failureDetectorThread = new Thread(new FailureDetectorThread());
		fileTransferThread = new Thread(new FileTransferThread());
		pregelMessageThread = new PregelMessageThread();
		workerThread = null;
		vertexHashMap = new ConcurrentHashMap<Integer, Vertex>();

		membershipList[thisNode.hashID] = thisNode;
	}

	/*
	 * Send a message to another node
	 * 
	 * @param destination: the host name of the target node
	 * 
	 * @param messageType: heartbeat, join, membershiplist, newmember, leave or
	 * failure
	 * 
	 * @param node: the node of the event
	 */
	private void sendMessage(String destination, int messageType, Node node) throws UnknownHostException {
		DatagramSocket ds;
		try {
			ds = new DatagramSocket();
			InetAddress serverAddress = InetAddress.getByName(destination);
			String message = nodeToMessage(messageType, node);
			DatagramPacket dp_send = new DatagramPacket(message.getBytes(), message.length(), serverAddress,
					SERVER_PORT);
			ds.send(dp_send);
			ds.close();
		} catch (SocketException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	private void sendMessage(String destination, int messageType, String sdfsfilename) throws UnknownHostException {
		DatagramSocket ds;
		try {
			ds = new DatagramSocket();
			InetAddress serverAddress = InetAddress.getByName(destination);
			String message = messageType + "\n" + sdfsfilename + "\n";
			DatagramPacket dp_send = new DatagramPacket(message.getBytes(), message.length(), serverAddress,
					SERVER_PORT);
			ds.send(dp_send);
			ds.close();
		} catch (SocketException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void sendMessage(String destination, int messageType, String sdfsfilename, String addr, long timestamp)
			throws UnknownHostException {
		DatagramSocket ds;
		try {
			ds = new DatagramSocket();
			InetAddress serverAddress = InetAddress.getByName(destination);
			String message = messageType + "\n" + sdfsfilename + "\n" + addr + "\n" + timestamp + "\n";
			DatagramPacket dp_send = new DatagramPacket(message.getBytes(), message.length(), serverAddress,
					SERVER_PORT);
			ds.send(dp_send);
			ds.close();
		} catch (SocketException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/*
	 * 
	 */
	private void nodeReset() {
		// heartBeatThread.interrupt();
		// messageHandlerThread.interrupt();
		// failureDetectorThread.interrupt();
		fileTransferThread.interrupt();
		goingToLeave = true;
		in_group = false;
		synchronized (fileRecords) {
			for (String filename : fileRecords.keySet()) {
				File file = new File(filename);
				if (file.exists())
					file.delete();
			}
		}

		try {
			Thread.currentThread().sleep(200);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		heartBeatThread = null;
	}

	/*
	 * Convert the message and the node information to a string of message
	 * 
	 * @param messageType: type of the message
	 * 
	 * @param node: the node of the event
	 */
	private String nodeToMessage(int messageType, Node node) {
		String message = messageType + "\n" + node.hostName + "\n" + node.timestamp + "\n" + node.hashID + "\n";
		return message;
	}

	/*
	 * Delete the node from local membership list and send the leave message to
	 * its successors
	 */
	public void memberLeave(Node node) {
		if (membershipList[node.hashID] != null) {
			synchronized (successors) {
				for (Node successor : successors) {
					try {
						sendMessage(successor.hostName, LEAVE, node);
					} catch (UnknownHostException e) {
						e.printStackTrace();
					}
				}
			}

			membershipList[node.hashID] = null;
			deleteNodeFileRecords(node.hostName);
			selectNeighbors();
			writeLogs("Member Leave:", node);
		}
	}

	public void deleteNodeFileRecords(String hostName) {
		synchronized (fileRecords) {
			for (String filename : fileRecords.keySet()) {
				List<FileRecord> list = fileRecords.get(filename);
				int i = 0;
				for (; i < list.size(); i++) {
					if (list.get(i).addr.equals(hostName))
						break;
				}
				if (i < list.size() && fileRecords.get(filename).size() > 0)
					list.remove(i);
			}
		}

	}

	/*
	 * Add the node to the local membership list and send the new group member
	 * message to its successors
	 */
	public void addMember(Node node) {
		if (membershipList[node.hashID] == null) {
			membershipList[node.hashID] = node;
			synchronized (successors) {
				for (Node successor : successors) {
					try {
						sendMessage(successor.hostName, NEWMEMBER, node);
					} catch (UnknownHostException e) {
						e.printStackTrace();
					}
				}
			}

			writeLogs("New Member:", node);
			selectNeighbors();
		}
	}

	public void initMembershipList(Node node) {
		in_group = true;
		synchronized (membershipList) {
			membershipList[node.hashID] = node;
		}
		selectNeighbors();
	}

	public void newGroupMember(Node node) {
		synchronized (membershipList) {
			for (Node member : membershipList) {
				try {
					if (member != null) {
						sendMessage(node.hostName, MEMBERSHIPLIST, member);
					}
				} catch (UnknownHostException e) {
					e.printStackTrace();
				}
			}
			membershipList[node.hashID] = node;
		}

		synchronized (successors) {
			for (Node successor : successors) {
				try {
					sendMessage(successor.hostName, NEWMEMBER, node);
				} catch (UnknownHostException e) {
					e.printStackTrace();
				}
			}
		}

		selectNeighbors();

		writeLogs("New Member:", node);
	}

	public void updateHBRecord(Node node) {
		synchronized (heartbeatCnt) {
			if (heartbeatCnt.containsKey(node.hashID)) {
				heartbeatCnt.put(node.hashID, System.currentTimeMillis());
			}
		}

	}

	/*
	 * Mark a node as failure and the send the failure message to its successors
	 */
	public void markAsFailure(Integer hashID) {

		if (membershipList[hashID] != null) {
			Node node = membershipList[hashID];
			if (node.hostName.equals("fa17-cs425-g15-01.cs.illinois.edu"))
				masterDead = true;

			synchronized (heartbeatCnt) {
				heartbeatCnt.remove(hashID);
			}

			synchronized (membershipList) {
				membershipList[hashID] = null;

			}
			deleteNodeFileRecords(node.hostName);
			selectNeighbors();

			synchronized (successors) {
				for (Node successor : successors) {
					try {
						sendMessage(successor.hostName, FAILURE, node);
					} catch (UnknownHostException e) {
						e.printStackTrace();
					}
				}
			}

			if (getHostName().equals("fa17-cs425-g15-01.cs.illinois.edu"))
				((PregelMessageThread) pregelMessageThread).startTask();
			else if (getHostName().equals("fa17-cs425-g15-02.cs.illinois.edu")) {
				if (masterDead)
					((PregelMessageThread) pregelMessageThread).startTask();
			} else {
				if (workerThread != null) {
					((WorkerThread) workerThread).setStop(true);
					workerThread.interrupt();
					workerThread = null;
				}
			}

			writeLogs("Failure:", node);
		}

	}

	/*
	 * Select successors and predecessors according to the new membership list
	 * and change heart beat target
	 */
	private void selectNeighbors() {
		synchronized (membershipList) {
			successors.clear();
			int index = (thisNode.hashID + 1) % 128;
			while (successors.size() < 3 && index != thisNode.hashID) {
				if (membershipList[index] != null)
					successors.add(membershipList[index]);
				index = (index + 1) % 128;
			}

			predecessors.clear();
			index = thisNode.hashID == 0 ? 127 : thisNode.hashID - 1;
			while (predecessors.size() < 3 && index != thisNode.hashID) {
				if (membershipList[index] != null)
					predecessors.add(membershipList[index]);
				index = index == 0 ? 127 : index - 1;
			}
		}
		synchronized (heartbeatCnt) {
			ConcurrentHashMap<Integer, Long> temp = heartbeatCnt;
			synchronized (predecessors) {
				heartbeatCnt = new ConcurrentHashMap<>();
				for (Node predecessor : predecessors) {
					if (temp.containsKey(predecessor.hashID))
						heartbeatCnt.put(predecessor.hashID, temp.get(predecessor.hashID));
					else {
						heartbeatCnt.put(predecessor.hashID, System.currentTimeMillis());
					}

				}
			}

		}

		new SendReplicaThread().start();

	}

	private void writeLogs(String event, Node node) {
		try {
			FileWriter writer = new FileWriter(getHostName() + ".log", true);
			writer.write(
					event + "\t" + node.hostName + "\t" + new Timestamp(System.currentTimeMillis()).toString() + "\n");
			writer.flush();
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void writeFileOpLog(String event, String filename) {
		try {
			FileWriter writer = new FileWriter(getHostName() + ".log", true);
			writer.write(event + "\t" + filename + "\t" + new Timestamp(System.currentTimeMillis()).toString() + "\n");
			writer.flush();
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void addFileRecord(String filename, String addr, long timestamp) {
		FileRecord fileRecord = new FileRecord(filename, addr, timestamp);
		synchronized (fileRecords) {
			if (fileRecords.containsKey(filename)) {
				for (int i = 0; i < fileRecords.get(filename).size(); i++) {
					if (fileRecords.get(filename).get(i).addr.equals(addr)) {
						fileRecords.get(filename).get(i).timestamp = timestamp;
						return;
					}
				}
				fileRecords.get(filename).add(fileRecord);
			} else {
				List<FileRecord> list = new LinkedList<>();
				list.add(fileRecord);
				fileRecords.put(filename, list);
			}
		}

	}

	public void deleteFile(String filename) {
		synchronized (fileRecords) {
			if (fileRecords.containsKey(filename)) {
				File file = new File(filename);
				if (file.exists() && file.isFile()) {
					file.delete();
					writeFileOpLog("Delete", filename);
				}
				fileRecords.remove(filename);
			}
		}

	}

	public void responseToReplica(Socket socket, BufferedReader socket_in, PrintWriter socket_out) throws IOException {
		String filename = socket_in.readLine();
		synchronized (fileRecords) {
			if (fileRecords.containsKey(filename)) {
				for (FileRecord fileRecord : fileRecords.get(filename)) {
					if (fileRecord.addr.equals(thisNode.hostName)) {
						socket_out.println("Cancel");
						return;
					}
				}
			}
		}

		socket_out.println("Continue");

		File file = new File(filename);
		// PrintWriter file_out = new PrintWriter(new FileWriter(file, false),
		// true);
		BufferedOutputStream file_out_s = new BufferedOutputStream(new FileOutputStream(filename));
		long timestamp = Long.parseLong(socket_in.readLine());

		System.out.println("Receiving replica...");
		BufferedInputStream socket_in_s = new BufferedInputStream(socket.getInputStream());
		byte[] line = new byte[4096];
		int len;
		while ((len = socket_in_s.read(line)) != -1) {
			file_out_s.write(line, 0, len);
		}
		file_out_s.flush();
		file_out_s.close();
		socket_in_s.close();
		System.out.println("Replica received");

		writeFileOpLog("Replica", filename);

		synchronized (membershipList) {
			for (Node node : membershipList) {
				if (node != null) {
					try {
						sendMessage(node.hostName, NEWFILERECORD, filename, thisNode.hostName, timestamp);
					} catch (UnknownHostException e) {
						e.printStackTrace();
					}
				}
			}
		}

		System.out.println("Replica broadcast complete");
	}

	public void responseToPut(Socket socket, BufferedReader socket_in, PrintWriter socket_out)
			throws NumberFormatException, IOException {
		String filename = socket_in.readLine();
		long timestamp = Long.parseLong(socket_in.readLine());

		synchronized (fileRecords) {
			if (fileRecords.containsKey(filename) && (timestamp != fileRecords.get(filename).get(0).timestamp)
					&& (timestamp - fileRecords.get(filename).get(0).timestamp < (long) (60 * 1000))) {
				socket_out.println("Confirmation");
				socket_out.flush();
				String line = socket_in.readLine();
				if (line.equals("Cancel"))
					return;
			} else {
				socket_out.println("continue");
			}
		}

		// PrintWriter file_out = new PrintWriter(new FileWriter(new
		// File(filename), false), true);

		BufferedOutputStream file_out_s = new BufferedOutputStream(new FileOutputStream(filename));
		BufferedInputStream socket_in_s = new BufferedInputStream(socket.getInputStream());
		byte[] line = new byte[4096];
		int len;
		while ((len = socket_in_s.read(line)) != -1) {
			file_out_s.write(line, 0, len);
		}
		file_out_s.flush();
		file_out_s.close();
		socket_in_s.close();

		writeFileOpLog("Update", filename);

		synchronized (membershipList) {
			for (Node node : membershipList) {
				if (node != null) {
					System.out.println("Sending new file record...");
					sendMessage(node.hostName, NEWFILERECORD, filename, thisNode.hostName, timestamp);
				}
			}
		}

	}

	public void responseToGet(Socket socket, BufferedReader socket_in, PrintWriter socket_out) throws IOException {

		String filename = socket_in.readLine();
		File file = new File(filename);
		synchronized (fileRecords) {
			if (!fileRecords.containsKey(filename) || !file.exists()) {
				socket_out.println("Not Found");
				return;
			} else {
				socket_out.println("Continue");
			}
		}

		try {
			Thread.currentThread().sleep(200);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		BufferedInputStream file_in_s = new BufferedInputStream(new FileInputStream(filename));
		BufferedOutputStream socket_out_s = new BufferedOutputStream(socket.getOutputStream());
		byte[] line = new byte[4096];
		int len;
		while ((len = file_in_s.read(line)) != -1) {
			socket_out_s.write(line, 0, len);
		}

		socket_out_s.flush();
		file_in_s.close();
		socket_out_s.close();

	}

	/*
	 * Get the host name of the node
	 * 
	 * @return string of host name
	 */
	private String getHostName() {
		return System.getenv("HOSTNAME");
	}

	/*
	 * Hash the input String and module by 128
	 * 
	 * @param string: combination of the timestamp and hostName of a node
	 * 
	 * @return hash results of the string module by 128 as hashID of the node
	 */
	public int getHashID(String ID) {
		int hashID = 7;
		for (int i = 0; i < ID.length(); i++) {
			hashID = (hashID * 31 + ID.charAt(i)) % 128;
		}
		return Math.abs(hashID % 128);

	}

	/*
	 * Get local time as timestamp
	 * 
	 * @return timestamp as string
	 */
	public String getTimestamp() {
		// Timestamp has millis while SimpleDateFormat not
		Timestamp timestamp = new Timestamp(System.currentTimeMillis());
		return timestamp.toString();
	}

}

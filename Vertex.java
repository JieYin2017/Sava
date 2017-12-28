import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

public class Vertex<VertexValue, MessageValue, EdgeValue> implements Serializable {

	protected int id;

	protected VertexValue value;

	protected int supersteps;

	protected List<Edge> outEdgeList;

	protected Queue<Message> messageList;

	public static int numVertices;

	public Vertex(int id, VertexValue value) {
		this.id = id;
		this.value = value;
		this.supersteps = 0;
		this.outEdgeList = new ArrayList<>();
		this.messageList = new LinkedList<>();
	}

	public int getVertex_id() {
		return this.id;
	}

	public VertexValue getValue() {
		return this.value;
	}

	public void setValue(VertexValue newValue) {
		this.value = newValue;
	}

	public void addSuperstep() {
		this.supersteps++;
	}

	public int getSuperStep() {
		return this.supersteps;
	}

	public void addOutEdge(Edge edge) {
		this.outEdgeList.add(edge);
	}

	public void addMessage(Message message) {
		this.messageList.offer(message);
	}

	public void clearOutEdgeList() {
		outEdgeList.clear();
	}

	public void clearMessageList() {
		messageList.clear();
	}

	// abstract methods

	// public void sendMessageToNei(int target_vertex_id, MessageValue
	// message_value){
	//
	// }

	public boolean compute(List<String> workerIDList, List<LinkedList<Message>> messageBuffer) {
		return false;
	}

	// @Override
	// public int compareTo(Object o) {
	//
	// return 0;
	// }

	// public static void main(String[] args) {
	//
	// }

}

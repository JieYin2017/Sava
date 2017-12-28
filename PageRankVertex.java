import java.io.IOException;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.ConnectException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.LinkedList;
import java.util.List;

public class PageRankVertex extends Vertex<Double, Double, Void> implements Serializable, Comparable<PageRankVertex> {

	public PageRankVertex(int vertex_id, Double vertex_value) {
		super(vertex_id, vertex_value);
	}

	@Override
	public boolean compute(List<String> workerIDList, List<LinkedList<Message>> messageBuffer) {
		boolean changed = false;
		if (supersteps >= 1 && supersteps < 20) {
			Message<Double> message;
			double sum = 0.0;
			while (true) {
				synchronized (messageList) {
					message = messageList.peek();
					if (message == null || message.getSuperstep() > supersteps)
						break;
					else if (message.getSuperstep() < supersteps) {
						System.out.println("===========message delayed!!!");
						messageList.poll();
						continue;
					}
					sum += message.getValue();
					changed = true;
					messageList.poll();
				}
			}
			value = 0.15 + 0.85 * sum;

		}
		if (supersteps < 20) {
			int n = outEdgeList.size();
			for (Edge edge : outEdgeList) {
				messageBuffer.get(edge.getTarget() % workerIDList.size())
						.add(new Message<Double>("neighborMessage", value / n, edge.getTarget(), supersteps + 1));
				// try {
				//
				// Socket socket = new Socket(workerIDList.get(edge.getTarget()
				// % workerIDList.size()), 9000);
				// ObjectOutput sout = new
				// ObjectOutputStream(socket.getOutputStream());
				// sout.writeObject(
				// new Message<Double>("neighborMessage", value / n,
				// edge.getTarget(), supersteps + 1));
				// sout.flush();
				// sout.close();
				// } catch (UnknownHostException e) {
				// e.printStackTrace();
				// return changed;
				// } catch (IOException e) {
				// e.printStackTrace();
				// return changed;
				// }
			}
		}
		supersteps++;

		if (supersteps == 1)
			return true;
		return changed;

	}

	@Override
	public int compareTo(PageRankVertex o) {

		double compareValue = o.value;

		if (value < compareValue)
			return 1;
		else if (value > compareValue)
			return -1;
		else
			return 0;
	}

}

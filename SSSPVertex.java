import java.io.IOException;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.LinkedList;
import java.util.List;

public class SSSPVertex extends Vertex<Integer, Integer, Integer> implements Serializable, Comparable<SSSPVertex> {

	public SSSPVertex(int id, Integer value) {
		super(id, value);

	}

	@Override
	public int compareTo(SSSPVertex arg0) {
		return value - arg0.value;
	}

	@Override
	public boolean compute(List<String> workerIDList, List<LinkedList<Message>> messageBuffer) {

		boolean changed = false;
		int minDist = getVertex_id() == 1 ? 0 : Integer.MAX_VALUE;
		Message<Integer> message;
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
				minDist = Math.min(minDist, message.getValue());
				changed = true;
				messageList.poll();
			}
		}

		if (minDist < getValue()) {
			setValue(minDist);
			for (Edge edge : outEdgeList) {
				messageBuffer.get(edge.getTarget() % workerIDList.size()).add(new Message<Integer>("neighborMessage",
						((Integer) edge.getValue()) + minDist, edge.getTarget(), supersteps + 1));

				// try {
				// Socket socket = new Socket(workerIDList.get(edge.getTarget()
				// % workerIDList.size()), 9000);
				// ObjectOutput sout = new
				// ObjectOutputStream(socket.getOutputStream());
				// sout.writeObject(new Message<Integer>("neighborMessage",
				// ((Integer) edge.getValue()) + minDist,
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

}

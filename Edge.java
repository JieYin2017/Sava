
public class Edge<EdgeValue, VertexValue> {

	// VertexID source;
	int tagert;
	EdgeValue value;

	public Edge(int target) {
		// this.source = source;
		this.tagert = target;
	}

	public Edge(int target, EdgeValue value) {
		// this.source = source;
		this.tagert = target;
		this.value = value;
	}

	public int getTarget() {
		return this.tagert;
	}

	public EdgeValue getValue() {
		return this.value;
	}

}

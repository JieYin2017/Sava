import org.apache.spark
import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx.GraphLoader
import org.apache.spark.graphx.{Graph, VertexId}

object SSSP {
  def main(args: Array[String]) {
  	val spark = SparkSession.builder.appName("SSSP").getOrCreate()

  	val sc = spark.sparkContext
  	
  	val graph = GraphLoader.edgeListFile(sc, "amazon.sub.txt")

    val sourceId: VertexId = 1

    val initialGraph = graph.mapVertices((id, _) =>
        if (id == sourceId) 0.0 else Double.PositiveInfinity)

	  val sssp = initialGraph.pregel(Double.PositiveInfinity)(
      (id, dist, newDist) => math.min(dist, newDist), // Vertex Program
      triplet => {  // Send Message
        if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
          Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
        } else {
          Iterator.empty
        }
      },
      (a, b) => math.min(a, b) // Merge Message
    )
   	
    println(sssp.vertices.collect.mkString("\n"))
   	

    //sssp.vertices.(Ordering.by[(org.apache.spark.graphx.VertexId, Int), Int](_._2)).foreach(println)
    
    spark.stop()

  }
}
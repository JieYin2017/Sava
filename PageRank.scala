import org.apache.spark
import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx.GraphLoader


object PageRank {
  def main(args: Array[String]) {
  	val spark = SparkSession.builder.appName("PageRank").getOrCreate()

  	val sc = spark.sparkContext
  	
  	val graph = GraphLoader.edgeListFile(sc, "amazon.sub.txt")

	val ranks = graph.staticPageRank(20, 0.15).vertices
   	
   	ranks.top(25)(Ordering.by[(org.apache.spark.graphx.VertexId, Double), Double](_._2)).foreach(println)
    
    spark.stop()

  }
}
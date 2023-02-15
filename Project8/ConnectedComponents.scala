import org.apache.spark.graphx.{Graph,Edge,EdgeTriplet,VertexId}
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object ConnectedComponents {
  def main ( args: Array[String] ) {
    val conf = new SparkConf().setAppName("Connected Components").setMaster("local[2]")
    val sc = new SparkContext(conf)

    // A graph G is a dataset of vertices, where each vertex is (id,adj),
    // where id is the vertex id and adj is the list of outgoing neighbors
    var G: RDD[ ( Long, List[Long] ) ] 
    // read the graph from the file args(0)
       = sc.textFile(args(0)).map(
          line => {
            val (node, neighbors) = line.split(",").splitAt(1)
            (node(0).toLong, neighbors.toList.map(_.toLong))
          }
        )
       /* ... */

    // graph edges have attribute values 0
    val edges: RDD[Edge[Long]] = G.flatMap( x => x._2.map(y => (x._1, y)))
                                  .map( nodes => Edge(nodes._1, nodes._2, nodes._1))
    /* ... */ 

    // a vertex (id,group) has initial group equal to id
    val vertices = Graph.fromEdges(edges, "defaultProperty") /* ... */

    // the GraphX graph
    val graph: Graph[Long,Long] = vertices.mapVertices((id, _) => id)

    // find the vertex new group # from its current group # and the group # from the incoming neighbors
    def newValue ( id: VertexId, currentGroup: Long, incomingGroup: Long ): Long
      = math.min(currentGroup, incomingGroup)
      /* ... */

    // send the vertex group # to the outgoing neighbors
    def sendMessage ( triplet: EdgeTriplet[Long,Long]): Iterator[(VertexId,Long)]
      = if (triplet.attr < triplet.dstAttr){
        Iterator((triplet.dstId, triplet.attr))
      }else if (triplet.srcAttr < triplet.attr){
        Iterator((triplet.dstId, triplet.srcAttr))
      }else{
        Iterator.empty;
      }
      /* ... */

    def mergeValues ( x: Long, y: Long ): Long
      = math.min(x, y)
      /* ... */

    // derive connected components using pregel
    val comps = graph.pregel (Long.MaxValue,5) (   // repeat 5 times
                      newValue,
                      sendMessage,
                      mergeValues
                   )

    // print the group sizes (sorted by group #)
    comps.vertices.map(newgraph => (newgraph._2, 1))
                  .reduceByKey(_+_)
                  .sortByKey()
                  .collect()
                  .map(a => a._1.toString+" "+a._2.toString)
                  .foreach(println)
    /* ... */
  }
  
}

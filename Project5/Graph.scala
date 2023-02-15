import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.collection.mutable.ListBuffer

object Graph {
  def main ( args: Array[String] ) {
    val conf = new SparkConf().setAppName("Graph")
    val sc = new SparkContext(conf)

    // A graph is a dataset of vertices, where each vertex is a triple
    //   (group,id,adj) where id is the vertex id, group is the group id
    //   (initially equal to id), and adj is the list of outgoing neighbors
    var graph =  sc.textFile(args(0)).map(line => {
                      var a = line.split(",")
                      var all = new ListBuffer[Long]()
                      for(i <- 1 to (a.length - 1)){
                        all += a(i).toLong
                      }
                      var adj = all.toList
                      (a(0).toLong, a(0).toLong, adj)

        })
       /* put your code here */      // read the graph from the file args(0)
    var this_graph = graph.map(f => (f._1, f))
    println(this_graph)
    for ( i <- 1 to 5 ) {
       // For each vertex (group,id,adj) generate the candidate (id,group)
       //    and for each x in adj generate the candidate (x,group).
       // Then for each vertex, its new group number is the minimum candidate
      graph = graph.flatMap(
                    f => {
                      var newAll = new ListBuffer[(Long, Long)]()
                      newAll += ((f._1, f._2)) // 
                      for(i <- 0 to (f._3.length - 1)){
                        newAll += ((f._3(i), f._2))
                      }
                      var newAdj = newAll.toList
                      (newAdj)  
                    }
            )
          .reduceByKey((t1, t2) => (if (t1 >= t2) t2 else t1))
          .join(this_graph)
          .map( f => (f._2._2._2, f._2._1, f._2._2._3))
          /* put your code here */

       // reconstruct the graph using the new group numbers
      //graph = groups.map(g => (g._2, 1)).reduceByKey((x, y) => (x + y))
      /* put your code here */
    }

    // print the group sizes
    val graphSize = graph.map(f => (f._2, 1)).reduceByKey((x, y) => (x + y)).sortByKey().collect().foreach(println)/* put your code here */

    sc.stop()
  }
}

import org.apache.spark._
import org.apache.spark.rdd._

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object Add {
  val rows = 100
  val columns = 100

  case class Block ( data: Array[Double] ) {
    override
    def toString (): String = {
      var s = "\n"
      for ( i <- 0 until rows ) {
        for ( j <- 0 until columns )
          s += "\t%.3f".format(data(i*rows+j))
        s += "\n"
      }
      s
    }
  }

  /* Convert a list of triples (i,j,v) into a Block */
  def toBlock ( triples: List[(Int,Int,Double)] ): Block = {

    val b = new Array[Double](rows*columns)
    for( (i,j,v) <- triples){
      b((i%rows)*rows+(j%columns)) = v
    }
    Block(b)
    /* ... */
  }

  /* Add two Blocks */
  def blockAdd ( m: Block, n: Block ): Block = {

    val data = new Array[Double](rows*columns)
    for( i <- 0 until rows){
      for (j <- 0 until columns){
        data((i%rows)*rows+(j%columns)) = m.data((i%rows)*rows+(j%columns)) + n.data((i%rows)*rows+(j%columns))
      }
    }
    Block(data)
    /* ... */
  }

  /* Read a sparse matrix from a file and convert it to a block matrix */
  def createBlockMatrix ( sc: SparkContext, file: String ): RDD[((Int,Int),Block)] = {

    val triples = sc.textFile(file).map( line => { val a = line.split(",")
                                            ((a(0).toInt/rows, a(1).toInt/columns), (a(0).toInt%rows, a(1).toInt%columns, a(2).toDouble))
                                            })
    triples.groupByKey().map( create => { 
                                val key = create._1
                                val block = toBlock(create._2.toList)
                                (key, block)
                              })

    /* ... */
  }

  def main ( args: Array[String] ) {

    val conf = new SparkConf().setAppName("Add")
    val sc = new SparkContext(conf)

    val m_matrix = createBlockMatrix(sc, args(0))
    val n_matrix = createBlockMatrix(sc, args(1))

    val res = m_matrix.join(n_matrix).map( total => {
                      val key = total._1
                      val block = blockAdd(total._2._1, total._2._2)
                      (key, block)
      })

    res.saveAsTextFile(args(2))
    sc.stop()
    /* ... */
  }
}

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object Netflix {

  def main ( args: Array[ String ]): Unit = {
    val conf = new SparkConf().setAppName("Netflix")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._

    val FC = spark.sparkContext.textFile(args(0))
    val Data = FC.filter(x => !x.endsWith(":")).toDF()
    //Data.select("*").show()
    val all_data = Data.select(split(col("value"),",").getItem(0).as("Users"), split(col("value"),",").getItem(1).as("Ratings"))
    all_data.createOrReplaceTempView("data")
    //all_data.select("*").show()
    val query = spark.sql("select Ratings, count(Ratings) as Count from (select Ratings/10 as Ratings from (select cast(Ratings as int) as Ratings from (select Users, (sum(Ratings)*10/count(Ratings)) as Ratings from data group by Users))) group by Ratings order by Ratings")
    query.show(Int.MaxValue)
    spark.stop()
    //substring(Ratings,0, instr(Ratings,'.')+1)
    /* ... */

  }
}

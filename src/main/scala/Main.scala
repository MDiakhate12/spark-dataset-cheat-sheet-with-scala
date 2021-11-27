import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level

object Main {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Spark Dataset Cheat Sheet with Scala")
      .getOrCreate()

    // # Load JSON
    val df = spark.read.option("multiline", "true").json("data/users.json")

    df.printSchema()
    df.show()
  }
}

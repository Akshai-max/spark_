import org.apache.spark.sql.SparkSession

object WordCount {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("WordCount").master("local[*]").getOrCreate()
    val sc = spark.sparkContext
    val text = sc.textFile("input.txt")
    val counts = text.flatMap(_.split("\\s+")).map((_, 1)).reduceByKey(_ + _)
    counts.collect().foreach(println)
    counts.saveAsTextFile("output")
    spark.stop()
  }
}

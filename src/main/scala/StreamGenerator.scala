import org.apache.spark.sql.SparkSession

object StreamGenerator {
  def main(args: Array[String]): Unit = {

    val nHits = 128
    val file = args(0)
    val nMessages = args(1).toInt

    val spark= SparkSession.builder().appName("binaryStreamGenerator").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val rdd = sc.binaryRecords(file, nHits*8)

    import spark.implicits._
    val df = rdd.toDF("value").limit(nMessages)

    df.write
      .format("kafka")
      .option("kafka.bootstrap.servers", "10.64.22.40:9092,10.64.22.41:9092,10.64.22.42:9092")
      .option("topic", "test80")
      .save()
  }
}

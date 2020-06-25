import org.apache.spark.sql.SparkSession

object StreamGenerator {
  def main(args: Array[String]): Unit = {

    var nHits = 128
    var file = " "
    var nMessages = 100
    var kafkaBrokers = " "
    var inputTopic = " "

    // Parse parameters
    args.sliding(2, 2).toList.foreach{
      case Array("--brokers", brokersIP: String) => {
        kafkaBrokers = brokersIP
        println(s"Brokers: $kafkaBrokers")
      }
      case Array("--input-topic", topic: String) => {
        inputTopic = topic
        println(s"Input topic: $inputTopic")
      }
      case Array("--file", file_path: String) => {
        file = file_path
        println(s"File used to simulate the stream: $file")
      }
      case Array("--num-messages", nmsg: String) => {
        nMessages = nmsg.toInt
        println(s"Number of messages used: $nMessages")
      }
      case _ => println("Invalid argument")
    }

    val spark= SparkSession.builder().appName("binaryStreamGenerator").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val rdd = sc.binaryRecords(file, (nHits+1)*8)

    import spark.implicits._
    val df = rdd.toDF("value").limit(nMessages)

    df.write
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBrokers)
      .option("topic", "test80")
      .save()
  }
}
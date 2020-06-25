import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{greatest, lit, min, when}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StreamProcessor {
  def main(args: Array[String]): Unit = {
    // Input parameters
    // Usage: --par_name value
    var inputTopic = "" // Kafka topic
    var kafkaBrokers = " " // Address of brokers
    var nHits = 128 // Number of hits per message
    var windowTime = 5 // Window seconds

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
      case Array("--num-hits", hits: String) => {
        nHits = hits.toInt
        println(s"Hits per message: $nHits")
      }
      case Array("--window", window: String) => {
        windowTime = window.toInt
        println(s"Window time: $windowTime")
      }
      case _ => println("Invalid argument")
    }

    val consumerConfig = Map[String, Object](
      "bootstrap.servers" -> kafkaBrokers,
      "key.deserializer" -> "org.apache.kafka.common.serialization.ByteArrayDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.ByteArrayDeserializer",
      "group.id" -> "42"
    )

    // Create streaming context
    val conf = new SparkConf().setAppName("DAQStream")
    val ssc = new StreamingContext(conf, Seconds(windowTime))
    ssc.sparkContext.setLogLevel("ERROR")

    // Create a direct stream
    val stream = KafkaUtils.createDirectStream[Array[Byte], Array[Byte]](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[Array[Byte], Array[Byte]](
        Array(inputTopic),
        consumerConfig
      )
    ).map(x => x.value())

    stream.foreachRDD(rdd => {

      // Measure batch processing time
      val startTimer = System.currentTimeMillis()

      if(!rdd.partitions.isEmpty){

        // Get the singleton instance of SparkSession
        val spark = SparkSessionSingleton.getInstance(rdd.sparkContext.getConf)
        import spark.implicits._

        // create df
        val df = rdd.toDF("records")

        //unpack df and remove non-physical hits
        val unpackedDataframe = Unpacker.unpack(df, nHits)
          .where($"TDC_CHANNEL"=!=139)

        val allhits = unpackedDataframe.filter("HEAD <= 2").drop("TRG_QUALITY")
        val triggershits = unpackedDataframe.filter("HEAD > 2")

        val triggers = triggershits
          .groupBy("ORBIT_CNT")
        val triggers_table = triggers.agg(min("TDC_MEAS").alias("T0"))

        val hits = allhits.join(triggers_table, "ORBIT_CNT")
          .withColumn("TDRIFT", ($"BX_COUNTER"-$"T0")*25 + $"TDC_MEAS"*25/30)
          .drop("T0")
          .where(($"TDRIFT"> -50) && ($"TDRIFT"<500))

        val NCHANNELS = 64
        val XCELL = 42.0
        val ZCELL = 13.0
        val TDRIFT = 15.6*25.0
        val VDRIFT = XCELL*0.5 / TDRIFT

        val events = hits.withColumn("X_POSSHIFT",
          when($"TDC_CHANNEL" % 4 === 1, 0)
            .when($"TDC_CHANNEL" % 4 === 2, 0)
            .when($"TDC_CHANNEL" % 4 === 3, 0.5)
            .when($"TDC_CHANNEL" % 4 === 0, 0.5)
            .otherwise(0.0)
        )
          .withColumn("SL",
            when(($"FPGA" === 0) && ($"TDC_CHANNEL" <= NCHANNELS) , 0)
              .when(($"FPGA" === 0) && ($"TDC_CHANNEL" > NCHANNELS) && ($"TDC_CHANNEL" <= 2*NCHANNELS), 1)
              .when(($"FPGA" === 1) && ($"TDC_CHANNEL" <= NCHANNELS) , 2)
              .when(($"FPGA" === 1) && ($"TDC_CHANNEL" > NCHANNELS) && ($"TDC_CHANNEL" <= 2*NCHANNELS), 3)
              .otherwise(-1)
          )
          .withColumn("TDC_CHANNEL_NORM", $"TDC_CHANNEL" - lit(NCHANNELS)*($"SL"%2))
          .withColumn("X_POS_LEFT", ((($"TDC_CHANNEL_NORM"-0.5)/4).cast("integer") +
            $"X_POSSHIFT")*XCELL + XCELL/2 - greatest($"TDRIFT", lit(0))*VDRIFT
          )
          .withColumn("X_POS_RIGHT", ((($"TDC_CHANNEL_NORM"-0.5)/4).cast("integer") +
            $"X_POSSHIFT")*XCELL + XCELL/2 + greatest($"TDRIFT", lit(0))*VDRIFT
          )
          .withColumn("Z_POS",
            when($"TDC_CHANNEL" % 4 === 1, ZCELL*3.5)
              .when($"TDC_CHANNEL" % 4 === 2, ZCELL*1.5)
              .when($"TDC_CHANNEL" % 4 === 3, ZCELL*2.5)
              .when($"TDC_CHANNEL" % 4 === 0, ZCELL*0.5)
              .otherwise(0.0)
          )

        // show events
        events.select("RUN_ID", "ORBIT_CNT", "X_POS_LEFT", "X_POS_RIGHT", "Z_POS", "TDRIFT").show(10)

        // to do: write somewhere
      }

      println("Processing Time: %.1f s\n".format((System.currentTimeMillis()-startTimer).toFloat/1000))
    })

    // Start the computation
    ssc.start()
    ssc.awaitTermination()

  }
}

/** Lazily instantiated singleton instance of SparkSession */
object SparkSessionSingleton {

  @transient  private var instance: SparkSession = _

  def getInstance(sparkConf: SparkConf): SparkSession = {
    if (instance == null) {
      instance = SparkSession
        .builder
        .config(sparkConf)
        .getOrCreate()
    }
    instance
  }
}

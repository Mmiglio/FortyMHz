import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{greatest, lit, min, when}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

object Application {
  def main(args: Array[String]): Unit = {
    //Input parameters
    var inputTopic = " " // Topic read from kafka
    var nHits = 128      // Hits contained per message
    var windowTime = 5   // Batch time in seconds

    try {
      inputTopic = args(0)
      nHits = args(1).toInt
      windowTime = args(2).toInt
    } catch {
      case e: Exception => {
        println("Wrong number of parameters")
        println("Input should be in the form <input topic> <numberOfHits> <batch-time (s)> ")
        System.exit(1)
      }
    }

    val consumerConfig = Map[String, Object](
      "bootstrap.servers" -> "10.64.22.40:9092,10.64.22.41:9092,10.64.22.42:9092",
      "key.deserializer" -> "org.apache.kafka.common.serialization.ByteArrayDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.ByteArrayDeserializer",
      "group.id" -> "40"
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

      if(!rdd.isEmpty()){

        // Get the singleton instance of SparkSession
        val spark = SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate()
        import spark.implicits._

        // create df
        val df = rdd.toDF("records")

        //unpack df and remove non-physical hits
        val unpackedDataframe = Unpacker.unpack(df, nHits)
          .where($"TDC_CHANNEL"=!=139)

        val allhits = unpackedDataframe.filter("HEAD == 1").drop("TRG_QUALITY")
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
        events.select("ORBIT_CNT", "X_POS_LEFT", "X_POS_RIGHT", "Z_POS", "TDRIFT").show(10)

        // to do: create json with columns and write to kafka
      }

      println("Processing Time: %.1f s\n".format((System.currentTimeMillis()-startTimer).toFloat/1000))
    })

    // Start the computation
    ssc.start()
    ssc.awaitTermination()

  }

}

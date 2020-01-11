import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{greatest, lit, min, when}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

object Application {
  def main(args: Array[String]): Unit = {

    val windowTime = args(0).toInt

    val nHits = 128 //number of hits per message
    val topicIn = Array("test80")
    val topicOut = Array("events")

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
        topicIn,
        consumerConfig
      )
    ).map(x => x.value())

    stream.foreachRDD(rdd => {

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

        // show events
        events.select("ORBIT_CNT", "X_POS_LEFT", "X_POS_RIGHT").show(10)

        // to do: create json with columns and write to kafka

      }

    })

    // Start the computation
    ssc.start()
    ssc.awaitTermination()

  }

}

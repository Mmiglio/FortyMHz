import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.streaming.{StreamingQuery, StreamingQueryListener, Trigger}
import org.apache.spark.sql.functions.{collect_list, greatest, lit, min, size, struct, when}
import org.apache.spark.storage.StorageLevel
import org.elasticsearch.spark.sql._

object StructuredStreaming {
  def main(args: Array[String]): Unit = {
    // Input parameters
    // Usage: --par_name value
    var inputTopic = "" // Kafka topic
    var kafkaBrokers = " " // Address of brokers
    var nHits = 128 // Number of hits per message
    var windowTime = 1000 // Window milliseconds

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

    val spark: SparkSession = SparkSession
      .builder
      .appName("40MHz")
      .getOrCreate()

    import spark.implicits._
    spark.sparkContext.setLogLevel("ERROR")

    spark.streams.addListener(new StreamingQueryListener {
      override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {}
      override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
        println("BatchID: " + event.progress.batchId)
        println(s"   * Number of input blocks/hits: ${event.progress.numInputRows.toInt} / ${event.progress.numInputRows.toInt*nHits}")
        println(s"   * Input blocks/s:    " + event.progress.inputRowsPerSecond.toInt)
        println(s"   * Processed block/s: " + event.progress.processedRowsPerSecond.toInt)
      }
      override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {}
    })

    val kafkaRecords = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBrokers)
      .option("subscribe", inputTopic)
      .load
      .select("value", "timestamp")
      .withColumnRenamed("value", "records")

    // unpack records
    val unpackedDataframe = Unpacker.unpack(kafkaRecords, nHits)
      .where($"TDC_CHANNEL"=!=139)

    val processorQuery: StreamingQuery = unpackedDataframe
      .writeStream
      .foreachBatch({(batchDF: DataFrame, batchId: Long) =>
        //val cachedBatchDF = batchDF.cache()
        val cachedBatchDF = batchDF.persist(StorageLevel.MEMORY_ONLY)
        //println(s"Batch: ${batchId}, hits: ${cachedBatchDF.count()}")

        //val startTimer = System.currentTimeMillis()

        // find hits and triggers based on header
        val allhits = cachedBatchDF.filter("HEAD <= 2").drop("TRG_QUALITY")
        val triggershits = cachedBatchDF.filter("HEAD > 2")

        val triggers_table = triggershits
          .groupBy("ORBIT_CNT")
          .agg(min("TDC_MEAS").alias("T0"))

        val hits = allhits.join(triggers_table, "ORBIT_CNT")
          .withColumn("TDRIFT", ($"BX_COUNTER"-$"T0")*25 + $"TDC_MEAS"*25/30)
          .withColumn("BX_TRIG", $"T0")
          .drop("T0")
          .where(($"TDRIFT"> -50) && ($"TDRIFT"<500))

        val NCHANNELS = 64
        val XCELL = 42.0
        val ZCELL = 13.0
        val TDRIFT = 15.6*25.0
        val VDRIFT = XCELL*0.5 / TDRIFT

        val eventBuilder = hits.withColumn("SL",
            when(($"FPGA" === 0) && ($"TDC_CHANNEL" <= NCHANNELS) , 0)
              .when(($"FPGA" === 0) && ($"TDC_CHANNEL" > NCHANNELS) && ($"TDC_CHANNEL" <= 2*NCHANNELS), 1)
              .when(($"FPGA" === 1) && ($"TDC_CHANNEL" <= NCHANNELS) , 2)
              .when(($"FPGA" === 1) && ($"TDC_CHANNEL" > NCHANNELS) && ($"TDC_CHANNEL" <= 2*NCHANNELS), 3)
              .otherwise(-1)
          )
          .withColumn("TDC_CHANNEL_NORM", $"TDC_CHANNEL" - lit(NCHANNELS)*($"SL"%2))
          .withColumn("LAYER",
            when($"TDC_CHANNEL_NORM" % 4 === 1, 1)
              .when($"TDC_CHANNEL_NORM" % 4 === 2, 3)
              .when($"TDC_CHANNEL_NORM" % 4 === 3, 2)
              .when($"TDC_CHANNEL_NORM" % 4 === 0, 4)
              .otherwise(0.0)
          )
          .withColumn("Z_POS",
            when($"TDC_CHANNEL_NORM" % 4 === 1, 1.5*ZCELL)
              .when($"TDC_CHANNEL_NORM" % 4 === 2, -0.5*ZCELL)
              .when($"TDC_CHANNEL_NORM" % 4 === 3, 0.5*ZCELL)
              .when($"TDC_CHANNEL_NORM" % 4 === 0, -1.5*ZCELL)
              .otherwise(0.0)
          )
          .withColumn("X_POSSHIFT",
            when($"TDC_CHANNEL_NORM" % 4 === 1, -7.5*XCELL)
              .when($"TDC_CHANNEL_NORM" % 4 === 2, -7.5*XCELL)
              .when($"TDC_CHANNEL_NORM" % 4 === 3, -7.0*XCELL)
              .when($"TDC_CHANNEL_NORM" % 4 === 0, -7.0*XCELL)
              .otherwise(0.0)
          )
          .withColumn("WIRE_NUM", ((($"TDC_CHANNEL_NORM"-1)/4).cast("integer") + 1)
          .withColumn("WIRE_POS", ($"WIRE_NUM"-1)*XCELL + $"X_POSSHIFT"))
          .withColumn("X_POS_LEFT", $"WIRE_POS" - greatest($"TDRIFT", lit(0))*VDRIFT)
          .withColumn("X_POS_RIGHT", $"WIRE_POS" + greatest($"TDRIFT", lit(0))*VDRIFT)

        // Dataframe with events list
        val events = eventBuilder.select("RUN_ID", "ORBIT_CNT", "BX_TRIG", "SL", "LAYER", "WIRE_NUM", "X_POS_LEFT", "X_POS_RIGHT", "Z_POS", "TDRIFT")

        // Aggregate by orbit
        val groupedEvents = events.groupBy("ORBIT_CNT", "run_id")
          .agg(collect_list(struct($"X_POS_LEFT", $"X_POS_RIGHT", $"Z_POS", $"TDRIFT")).as("HITS_LIST")) //struct(events.columns.head, events.columns.tail: _*)
          .withColumn("NHITS", size($"HITS_LIST"))

        groupedEvents
          .withColumn("TIME_STAMP", lit(System.currentTimeMillis))
          .select("RUN_ID", "TIME_STAMP", "ORBIT_CNT", "NHITS", "HITS_LIST")
          .saveToEs("run-{RUN_ID}")

        //println("Processing Time: %.1f s\n".format((System.currentTimeMillis()-startTimer).toFloat/1000))

        cachedBatchDF.unpersist()
      })
      .trigger(Trigger.ProcessingTime(windowTime))
      .start()

    processorQuery.awaitTermination()

  }
}

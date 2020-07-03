import java.nio.{ByteBuffer, ByteOrder}
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{udf,lit,explode,col}

object Unpacker {

  def unpack(df: DataFrame, nwords: Int): DataFrame = {
    // Convert message (nhits + runID) binaries
    val convertUDF = udf((record: Array[Byte], nwords: Int) => {
      val bb = ByteBuffer.wrap(record).order(ByteOrder.LITTLE_ENDIAN)
      val hits = ArrayBuffer[Array[Long]]()

      // get first word containing run id
      val hit = bb.getLong
      // check head
      if (((hit >> 62) & 0x3) != 0) println("Error! head != 0")
      // get run id
      val runID = hit & 0xFFFFFFFF

      for (_ <- 1 to nwords){
        val hit = bb.getLong
        val HEAD = (hit >> 62) & 0x3

        // Hit
        if(HEAD<=2){
          hits.append(hitUnpacker(hit))
        }
        // Trigger
        else if(HEAD==3){
          hits.append(triggerUnpacker(hit))
        }
      }
      // return array of hits + runID
      hits.map(hit => runID +: hit)
    })

    val unpackedDF: DataFrame = df.withColumn("converted", convertUDF(df("records"), lit(nwords)))
      .withColumn("converted", explode(col("converted")))
      .select(
        //col("timestamp"),
        col("converted")(0).as("RUN_ID"),
        col("converted")(1).as("HEAD"),
        col("converted")(2).as("FPGA"),
        col("converted")(3).as("TDC_CHANNEL"),
        col("converted")(4).as("ORBIT_CNT"),
        col("converted")(5).as("BX_COUNTER"),
        col("converted")(6).as("TDC_MEAS"),
        col("converted")(7).as("TRIG_QUAL")
      )

    unpackedDF
  }

  def hitUnpacker(hit: Long): Array[Long] = {

    val HEAD = (hit >> 62) & 0x3
    val FPGA = asUInt((hit >> 58) & 0xF)
    val TDC_CHANNEL = asUInt((hit >> 49) & 0x1FF) + 1
    val ORBIT_CNT = asUInt((hit >> 17) & 0xFFFFFFFF)
    val BX_COUNTER = asUInt((hit >> 5) & 0xFFF)
    var TDC_MEAS = asUInt((hit >> 0) & 0x1F)

    if ((TDC_CHANNEL != 137) && (TDC_CHANNEL != 138)) TDC_MEAS -= 1

    // -1 used in order to have 7 columns
    Array(HEAD, FPGA, TDC_CHANNEL, ORBIT_CNT, BX_COUNTER, TDC_MEAS, -1)
  }

  def triggerUnpacker(hit: Long): Array[Long] = {

    val HEAD = (hit >> 62) & 0x3 //check --> (hit & 0xC000000000000000L) >> 62
    val TRIG_MINI_CH = (hit & 0x3000000000000000L) >> 60
    val TRIG_MCELL = (hit & 0x0E00000000000000L) >> 57
    val TRIG_TAG_ORBIT = (hit & 0x01FFFFFFFE000000L) >> 25
    val TRIG_TAG_BX = (hit & 0x0000000001FFE000) >> 13
    val TRIG_BX = (hit & 0x0000000000001FFE) >> 1
    val TRIG_QUAL = (hit & 0x0000000000000001) >> 0

    Array(HEAD, TRIG_MINI_CH, TRIG_MCELL, TRIG_TAG_ORBIT, TRIG_TAG_BX, TRIG_BX, TRIG_QUAL)
  }

  def asUInt(longValue: Long): Long = {
    longValue & 0x00000000FFFFFFFFL
  }

}

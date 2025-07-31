package part2structuredstreaming

import common._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}

import scala.concurrent.duration._

object StreamingDataFrames {

  val spark = SparkSession
    .builder()
    .appName("Our First Streams")
    .config("spark.master", "local[2]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  def readFromSocket(): Unit = {
    // reading a DF
    val lines: DataFrame = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()

    // transformation
    val shortLines: DataFrame = lines.filter(length(col("value")) <= 5)

    // tell between a static vs a streaming DF
    println(shortLines.isStreaming)

    // consuming a DF
    val query: StreamingQuery = shortLines.writeStream
      .format("console")
      .outputMode("append")
      .start() // synchronous

    // wait for the stream to finish
    query.awaitTermination()
  }

  def readFromFiles() = {
    val stocksDF: DataFrame = spark.readStream
      .format("csv")
      .option("header", "false")
      .option("dateFormat", "MMM d yyyy")
      .schema(stocksSchema)
      .load("src/main/resources/data/stocks")

    stocksDF.writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
  }

  def demoTriggers() = {
    val lines: DataFrame = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()

    // write the lines DF at a certain trigger
    lines.writeStream
      .format("console")
      .outputMode("append")
      .trigger(
//        Trigger.ProcessingTime(2.seconds) // every 2 seconds, run the query
//        Trigger.Once() // single batch, then terminate
        Trigger.Continuous(
          2.seconds // every 2 seconds, create a batch with whatever you have
        )
      )
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    demoTriggers()
  }
}

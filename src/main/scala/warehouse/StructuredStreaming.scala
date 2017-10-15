package warehouse

import warehouse.util.CSVFileStreamGenerator

object StructuredStreaming {

  def main (args: Array[String]): Unit = {

    lazy val dataWarehouse = DataWarehouse

    val stream = new CSVFileStreamGenerator(10, 5, 500)
    val dfStream = dataWarehouse.readDataStream(stream)

    val dfGrouped = dfStream.groupBy("key").count()

    val query = dfGrouped.writeStream
      .outputMode("complete") // only allowed when aggregating
      .format("console")
      .start()

    Thread.sleep(5000)

    stream.makeFiles()

    Thread.sleep(5000)

    println("*** Stopping stream")

    query.stop()

    query.awaitTermination()
    println("*** Streaming terminated")
  }
}

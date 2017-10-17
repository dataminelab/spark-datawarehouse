package warehouse

import java.sql.Timestamp
import java.util.Properties

import org.apache.spark.sql._
import warehouse.util.CSVFileStreamGenerator

case class BidData(dateTime: Timestamp = null,
                   exchange: String = "",
                   trader: String = "",
                   traderId: Long = 0,
                   domain: String = "",
                   country: String = "",
                   device: String = "",
                   requests: Long = 0,
                   bids: Long = 0,
                   timeouts: Long = 0,
                   revenue: Double = 0.0,
                   avgTime: Double = 0.0
                  ) {}

/**
  * To generate sample data, see: https://next.json-generator.com/V1LZPRcnX
  */
object DataWarehouse {

  import org.apache.spark.sql.SparkSession
  import org.apache.spark.sql.functions._

  lazy val spark: SparkSession =
    SparkSession
      .builder()
      .appName("DataWarehouse")
      .config("spark.master", "local[4]")
      .getOrCreate()

  import spark.implicits._

  lazy val sc = spark.sparkContext

  def readData(): DataFrame = {
    SourceData.readBidsDataJson(spark)
  }

  def readDataXml(): DataFrame = {
    SourceData.readBidsDataXml(spark)
  }

  def readDataStream(stream: CSVFileStreamGenerator): DataFrame = {
    SourceData.readBidsDataStreaming(spark, stream)
  }

  /** Returns the list of dimensions and measures names on which we want to perform the ETL aggregation
    */
  def dimensionMeasureColumns() = {
    val dateDim = "dateTime"
    val dimensions = List("exchange", "device")
    val measures = Map(
      "requests" -> "sum",
      "bids" -> "sum",
      "timeouts" -> "sum",
      "revenue" -> "sum",
      "avg_time" -> "avg"
    )
    (dateDim, dimensions, measures)
  }

  /** Returns the number of bids per each exchange (in total) and order
    * by sum of bids in descending order
    *
    *  Hint1: consider using method `groupBy` and aggregate using `sum`
    *  Hint2: consider using method `orderBy`
    */
  def bidsPerExchange(df: DataFrame): DataFrame = {
    ???
  }

  /** Returns the number of bids per each exchange (in total),
    * sorted by sum of bids in the descending order
    *
    *  Hint1: Use SQL to perform these calculations
    *  Hint2: Use `df.createOrReplaceTempView` to register dataframe as a table
    *  Hint3: Use `spark.sql` to execute query
    */
  def bidsPerExchangeSql(df: DataFrame): DataFrame = {
    ???
  }

  /** Returns sum of all measures grouped per `dimensionColumns` dimensions and per day
    *  Hint1: To group by day use `date_format` function and convert it into yyyy-MM-dd format
    *  Hint2: (Optional) To pass element of the list as arguments use: mylist:_*
    *         See: https://stackoverflow.com/a/25815811/328989
    */
  def measuresGroupedByDimensions(df: DataFrame): DataFrame = {
    val (dateDim, dimensionColumns, measureColumns) = dimensionMeasureColumns()

    ???
  }

  /**
    * @return A `Dataset[BidData]` from the “untyped” `DataFrame`
    *
    * Hint: use the `map` method of DataFrame and convert row into BidData
    */
  def bidDataTyped(df: DataFrame): Dataset[BidData] =
    ???

  /** Returns sum of all measures grouped per all dimensions (per hour) as a dataset.
    *
    *  Hint1: Use `typed.sum` and `as` to provide the type hint
    *  Hint2: After ordering map the results back into the Dataset[BidData] using `map`
    */
  def bidsPerExchangeDataset(ds: Dataset[BidData]): Dataset[(String, Double)] = {
    ???
  }

  def main(args: Array[String]) {

    val dwDF: DataFrame = readData

    /* Number of bids on each exchange */
    timed("Number of bids by top exchanges", bidsPerExchange(dwDF).take(10).foreach(println))


    /* Output the speed of each ranking */
    println(timing)
    sc.stop()
  }

  val timing = new StringBuffer
  def timed[T](label: String, code: => T): T = {
    val start = System.currentTimeMillis()
    val result = code
    val stop = System.currentTimeMillis()
    timing.append(s"Processing $label took ${stop - start} ms.\n")
    result
  }
}

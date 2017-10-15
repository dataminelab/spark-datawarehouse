package warehouse

import org.apache.spark.sql.DataFrame
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import warehouse.DataWarehouse.dimensionMeasureColumns
import warehouse.util.CSVFileStreamGenerator

@RunWith(classOf[JUnitRunner])
class DataWarehouseTestSuite extends FunSuite with BeforeAndAfterAll {

  lazy val dataWarehouse = DataWarehouse
  lazy val sourceData = SourceData

  lazy val bidsDf = dataWarehouse.readData()
  lazy val bidsDfXml = dataWarehouse.readDataXml()

  val stream = new CSVFileStreamGenerator(10, 5, 500)
  lazy val dfStream = dataWarehouse.readDataStream(stream)

  val (dimDate, dimensionColumns, measureColumns) = dimensionMeasureColumns()

  lazy val bidsPerExchange: DataFrame = dataWarehouse.bidsPerExchange(bidsDf)
  lazy val bidsPerExchangeSql: DataFrame = dataWarehouse.bidsPerExchangeSql(bidsDf)

  lazy val measuresGroupedDf: DataFrame = dataWarehouse.measuresGroupedByDimensions(bidsDf)

  lazy val bidsDs = dataWarehouse.bidDataTyped(bidsDf)

  def initializeEnronAnalysis(): Boolean =
    try {
      DataWarehouse
      true
    } catch {
      case ex: Throwable =>
        println(ex.getMessage)
        ex.printStackTrace()
        false
    }

  override def afterAll(): Unit = {
    assert(initializeEnronAnalysis())
    import DataWarehouse._
    sc.stop()
  }

  test("dataWarehouse") {
    assert(dataWarehouse.spark.sparkContext.appName === "DataWarehouse")
    assert(dataWarehouse.spark.sparkContext.isStopped === false)
  }

  test("read") {
    assert(bidsDf.count === 100)
    bidsDf.show()
  }

  test("readXml") {
    assert(bidsDfXml.count === 100)
    bidsDfXml.show()
  }

  test("bidsPerExchange") {
    assert(bidsPerExchange.columns.length === 2)
    assert(bidsPerExchange.count === 5)
    bidsPerExchange.show()
  }

  test("bidsPerExchangeSql") {
    assert(bidsPerExchangeSql.columns.length === 2)
    assert(bidsPerExchangeSql.count === 5)
    bidsPerExchangeSql.show()
  }

  test("measuresGroupedByDimensions") {
    assert(measuresGroupedDf.columns.length === 8)
    assert(measuresGroupedDf.count === 28)
    //measuresGroupedDf.show()
  }

  test("timeUsageSummaryTyped"){
    assert(bidsDs.head.getClass.getName === "warehouse.BidData")
    assert(bidsDs.count === 100)
    bidsDs.show()
  }

  ignore("writeDataToJdbc"){
    sourceData.writeDataToJdbc(measuresGroupedDf, "bids_summarised")
  }

}

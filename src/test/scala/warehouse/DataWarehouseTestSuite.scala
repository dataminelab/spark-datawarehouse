package warehouse

import org.apache.spark.sql.DataFrame
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import warehouse.DataWarehouse.{dimensionMeasureColumns}
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

  ignore("dataWarehouse") {
    assert(dataWarehouse.spark.sparkContext.appName === "DataWarehouse")
    assert(dataWarehouse.spark.sparkContext.isStopped === false)
  }

  ignore("read") {
    assert(bidsDf.count === 100)
    bidsDf.show()
  }

  ignore("readXml") {
    assert(bidsDfXml.count === 100)
    bidsDfXml.show()
  }

  ignore("bidsPerExchange") {
    assert(bidsPerExchange.columns.length === 2)
    assert(bidsPerExchange.count === 5)
    bidsPerExchange.show()
  }

  ignore("bidsPerExchangeSql") {
    assert(bidsPerExchangeSql.columns.length === 2)
    assert(bidsPerExchangeSql.count === 5)
    bidsPerExchangeSql.show()
  }

  ignore("measuresGroupedByDimensions") {
    assert(measuresGroupedDf.columns.length === 8)
    assert(measuresGroupedDf.count === 28)
    //measuresGroupedDf.show()
  }

  ignore("timeUsageSummaryTyped") {
    assert(bidsDs.head.getClass.getName === "warehouse.BidData")
    assert(bidsDs.count === 100)
    bidsDs.show()
  }

  ignore("writeDataToJdbc") {
    sourceData.writeDataToJdbc(measuresGroupedDf, "bids_summarised")
  }

  ignore("writeDataToParquet") {
    measuresGroupedDf.show()
    sourceData.
      writeDataToParquet(measuresGroupedDf, "/tmp/sample.parquet")
  }

  ignore("readWriteRedshift") {

    val query =
      s"""
      select sum(lo_revenue), d_year, p_brand1
         from lineorder, dwdate, part, supplier
         where lo_orderdate = d_datekey
         and lo_partkey = p_partkey
         and lo_suppkey = s_suppkey
         and p_category = 'MFGR#12'
         and s_region = 'AMERICA'
         group by d_year, p_brand1
         order by d_year, p_brand1
         limit 1000
    """
    val df1 = sourceData.readDfFromRedshiftQuery(dataWarehouse.spark, query)
    df1.createOrReplaceTempView("dwdate_redshift")

    // write results into the new table
    sourceData.writeDfToRedshift(df1, dataWarehouse.spark, "brand_revenue")
  }
}

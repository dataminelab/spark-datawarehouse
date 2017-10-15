package warehouse

import java.nio.file.Paths
import java.util.Properties

import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import warehouse.util.CSVFileStreamGenerator

object SourceData {

  // See: https://github.com/databricks/spark-redshift/tree/master/tutorial
  // These should be stored in properties file or passed via args/environment
  val redshiftJdbcUrl = "jdbc:redshift://<replace-with-host>:5439/test?user=<username>&password=<password>"
  val awsAccessKeyId = ""
  val awsSecretAccessKey = ""
  val tempS3Dir = "s3n://radek-training/tempSpark/"

  def readDataFromParquet(spark: SparkSession, path: String) = {
    spark.conf.set("spark.sql.parquet.binaryAsString", "true");
    spark.read
      .option("mergeSchema", "true")
      .format("parquet")
      .load(path)
  }

  def writeDataToParquet(df: DataFrame, path: String) = {
    df.write.format("parquet").save(path)
  }

  def readDfFromRedshiftTable(spark: SparkSession, table: String) = {
    loadRedshiftData(spark)
      .option("dbtable", table)
      .load()
  }

  def readDfFromRedshiftQuery(spark: SparkSession, query: String) = {
    loadRedshiftData(spark)
      .option("query", query)
      .load()
  }

  private def loadRedshiftData(spark: SparkSession) = {
    spark.sparkContext.hadoopConfiguration.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
    spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", awsAccessKeyId)
    spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", awsSecretAccessKey)

    spark.sqlContext.read
      .format("com.databricks.spark.redshift")
      .option("url", redshiftJdbcUrl)
      .option("tempdir", tempS3Dir)
      .option("fs.s3n.awsAccessKeyId", awsAccessKeyId)
  }

  def writeDfToRedshift(df: DataFrame, spark: SparkSession, table: String,
                        mode: SaveMode = SaveMode.Overwrite) = {
    spark.sparkContext.hadoopConfiguration.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
    spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", awsAccessKeyId)
    spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", awsSecretAccessKey)

    df.write.format("com.databricks.spark.redshift")
      .option("url", redshiftJdbcUrl)
      .option("dbtable", table)
      //.option("forward_spark_s3_credentials", true)
      .option("tempdir", tempS3Dir)
      .mode(mode)
      .save()
  }

  def writeDataToJdbc(df: DataFrame, table: String) = {
    // These should be moved to the properties file
    val props = new Properties()
    props.setProperty("user", "root")
    props.setProperty("password", "example")
    props.setProperty("driver", "com.mysql.jdbc.Driver")

    val url = "jdbc:mysql://localhost:3306/spark"

    df.write.mode("overwrite").jdbc(url, table, props)
  }

  def readDataFromParquet(spark: SparkSession, path: String) = {
    spark.conf.set("spark.sql.parquet.binaryAsString", "true");
    spark.read
      .option("mergeSchema", "true")
      .format("parquet")
      .load(path)
  }

  def writeDataToParquet(df: DataFrame, path: String) = {
    df.write.format("parquet").save(path)
  }

  /** Use a combination of `spark.read.json`, and `resourcePath`
    *
    */
  def readBidsDataJson(spark: SparkSession): DataFrame = {
    // Note that the file that is offered as a json file is not a typical JSON file.
    // Each line must contain a separate, self-contained valid JSON object.
    // As a consequence, a regular multi-line JSON file will most often fail.
    //spark.read.json(resourcePath("analytics.json"))
    // If your file in multi-line use the following
    val df = spark.read.json(spark.sparkContext.wholeTextFiles(resourcePath("analytics.json")).values)
    cleanData(df)
  }

  /**
    * Clean up the data - remove $ from revenues "$113.98"
    *                   - turn "avg_time": "719.89" into Double
    */
  private def cleanData(df: DataFrame) = {
    //df.show()
    df.withColumn("dateTime", df("dateTime").cast(TimestampType))
      // Remove the $ from the revenue
      .withColumn("revenue", substring_index(df("revenue"), "$", -1).cast(DoubleType))
      .withColumn("avg_time", df("avg_time").cast(DoubleType)).cache()
  }

  /** Read XML data
    *
    */
  def readBidsDataXml(spark: SparkSession): DataFrame = {
    val df = spark.sqlContext.read.format("com.databricks.spark.xml")
      .option("rowTag", "row")
      .load(resourcePath("analytics.xml"))

    cleanData(df)
  }

  private def readDfFromJdbc(spark: SparkSession): DataFrame = {
    spark.sqlContext.read.format("jdbc")
      .option("url", "jdbc:mysql://127.0.0.1/warehouse")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("dbtable", "analytics")
      .option("user", "root")
      .option("password", "example").load()
  }

  /** Read streaming data
    *
    */
  def readBidsDataStreaming(spark: SparkSession, stream: CSVFileStreamGenerator): DataFrame = {

    val recordSchema = StructType(
      Seq(
        StructField("key", StringType),
        StructField("value", IntegerType)
      )
    )

    spark
      .readStream
      .option("sep", ",")
      .schema(recordSchema)
      .format("csv")
      .load(stream.dest.getAbsolutePath)
  }

  def resourcePath(resource: String): String =
    Paths.get(getClass.getResource(resource).toURI).toString

}

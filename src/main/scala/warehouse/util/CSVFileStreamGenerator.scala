package warehouse.util

import java.io.File
import java.sql.Timestamp
import java.time.{Duration, LocalDate}
import java.util.Date
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import java.util.Locale

import org.scalacheck.{Arbitrary, Gen}
import com.danielasfregola.randomdatagenerator.RandomDataGenerator._

import warehouse.BidData

/**
  * A utility for creating a sequence of files of integers in the file system
  * so that Spark can treat them like a stream. This follows a standard pattern
  * to ensure correctness: each file is first created in another folder and then
  * atomically renamed into the destination folder so that the file's point of
  * creation is unambiguous, and is correctly recognized by the streaming
  * mechanism.
  *
  * Each generated file has the same number of key/value pairs, where the
  * keys have the same names from file to file, and the values are random
  * numbers, and thus vary from file to file.
  *
  * This class is used by several of the streaming examples.
  *
  * See original:
  * https://github.com/spirom/LearningSpark/blob/master/src/main/scala/streaming/util/CSVFileStreamGenerator.scala
  */
class CSVFileStreamGenerator(nFiles: Int, nRecords: Int, betweenFilesMsec: Int) {

  private val root =
    new File(File.separator + "tmp" + File.separator + "streamFiles")
  makeExist(root)

  private val prep =
    new File(root.getAbsolutePath() + File.separator + "prep")
  makeExist(prep)

  val dest =
    new File(root.getAbsoluteFile() + File.separator + "dest")
  makeExist(dest)

  // fill a file with random bid data
  private def writeOutput(f: File): Unit = {

    val start = getZoneDateTime("2017-01-01")

    import com.fortysevendeg.scalacheck.datetime.GenDateTime._
    import com.fortysevendeg.scalacheck.datetime.instances.jdk8._

    implicit val arbitraryBidData: Arbitrary[BidData] = Arbitrary {
      for {
        dateTime <- genDateTimeWithinRange(
          start.plusMinutes(5), Duration.ofHours(23)
        ).map(t => Date.from(t.toInstant))
        exchange <- Gen.oneOf(List("NASDAQ", "NYSE", "BSE", "LSE", "TSX"))
        trader <- Gen.oneOf(List("ABC", "BCD", "CDE", "DEF", "EFG"))
        traderId <- Gen.choose(1000, 1010)
        domain <- Gen.oneOf(List(".UK", ".US", ".LV", ".DE", ".FR"))
        country <- Gen.oneOf(List("UK", "US", "Latvia", "Germany", "France"))
        device <- Gen.oneOf(List("Dekstop", "Mobile", "Tablet"))
        requests <- Gen.choose(20, 40)
        bids <- Gen.choose(10, 20)
        timeouts <- Gen.choose(5, 10)
        revenue <- Gen.choose(10, 200)
        avgTime <- Gen.choose(100, 1000)
      } yield BidData(new Timestamp(dateTime.getTime), exchange, trader, traderId, domain, country,
        device, requests, bids, timeouts, revenue, avgTime)
    }

    val p = new java.io.PrintWriter(f)
    try {
      val bidDataRecords = random[BidData](nRecords)
      for (b <- bidDataRecords) {
        p.println(s"$b.dateTime,$b.exchange,$b.trader,$b.traderId,$b.domain,$b.country," +
          s"$b.requests,$b.bids,$b.timeouts,$b.device,$b.revenue,$b.avgTime")
        //p.println(s"Key_$i,${Random.nextInt}")
      }
    } finally {
      p.close()
    }
  }

  private def getZoneDateTime(dateTime: String) = {
    val date = LocalDate.parse(dateTime,
      DateTimeFormatter.ofPattern("yyyy-MM-dd", Locale.ENGLISH))
    date.atStartOfDay(ZoneOffset.UTC)
  }

  private def makeExist(dir: File): Unit = {
    dir.mkdir()
  }

  // make the sequence of files by creating them in one place and renaming
  // them into the directory where Spark is looking for them
  // (file-based streaming requires "atomic" creation of the files)
  def makeFiles() = {
    for (n <- 1 to nFiles) {
      val f = File.createTempFile("Spark_", ".txt", prep)
      writeOutput(f)
      val nf = new File(dest + File.separator + f.getName)
      f renameTo nf
      nf.deleteOnExit()
      Thread.sleep(betweenFilesMsec)
    }
  }


}

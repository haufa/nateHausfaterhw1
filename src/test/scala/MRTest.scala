import MessageDistributionOverTimeInterval.{Map, Reduce, runMapReduce}
import ErrorMessageInTimeIntervalMR.*
import Parameters.*
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapred.{FileInputFormat, FileOutputFormat, JobClient, JobConf, TextInputFormat, TextOutputFormat}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.PrivateMethodTester
import org.scalatest.matchers.*

import scala.io.Source
import java.nio.file.{Paths, Files}
import java.time.LocalTime
import language.deprecated.symbolLiterals
import org.scalatest.matchers.should.Matchers

class MRTest extends AnyFlatSpec with Matchers with PrivateMethodTester {
  behavior of "Map-Reducer"


  private val inputToLog: String = "src/main/resources/logFileBig.log"
  private val outputPath: String = "src/test/output/outdir"


  val run = MessageDistributionOverTimeInterval.runMapReduce(inputToLog, outputPath+"1")
  it should "generate a file with 4 different message types-MR1" in {
    val lines: Iterator[String] = Source.fromFile(outputPath + "1/part-00000.csv").getLines
    lines.foreach { token =>
      token should contain
      "WARN|DEBUG|INFO|ERROR"
    }
  }

  it should "contain a valid log file and output-MR1" in {
    Files.exists(Paths.get(inputToLog)) shouldBe true
    Files.exists(Paths.get(outputPath+"1/part-00000.csv")) shouldBe true
    Files.exists(Paths.get(outputPath+"1/_SUCCESS")) shouldBe true
  }

  it should "contain a valid regex pattern-MR1" in {
    val patternString = Parameters.generatingPattern
    patternString.length should be >= 10;
  }

  it should "contain a valid time period-MR1" in {
    val start: LocalTime = LocalTime.parse(Parameters.startTime)
    val end: LocalTime = LocalTime.parse(Parameters.endTime)
    start.isBefore(end) shouldBe true
  }

  val run2 = ErrorMessageInTimeIntervalMR.runMapReduce(inputToLog, outputPath+"2")
  it should "contain a valid log file and output-MR2" in {
    Files.exists(Paths.get(inputToLog)) shouldBe true
    Files.exists(Paths.get(outputPath + "2/part-00000.csv")) shouldBe true
    Files.exists(Paths.get(outputPath + "2/_SUCCESS")) shouldBe true
  }
  it should "generate a file with 4 different message types-MR2" in {
    val lines: Iterator[String] = Source.fromFile(outputPath + "2/part-00000.csv").getLines
    lines.foreach { token =>
      token should contain
      "WARN|DEBUG|INFO|ERROR"
    }
  }


};
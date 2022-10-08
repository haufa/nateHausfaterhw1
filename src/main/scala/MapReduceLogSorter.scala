import org.apache.*
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.*
import org.apache.hadoop.io.*
import org.apache.hadoop.io.IntWritable.*
import org.apache.hadoop.util.*
import org.apache.hadoop.mapred.*
import org.slf4j.{Logger, LoggerFactory}

import java.io.*
import java.util
import scala.collection.immutable.ListMap
import com.typesafe.config.{Config, ConfigFactory}
import io.netty.handler.codec.DateFormatter

import java.time.{LocalTime}
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import scala.compiletime.ops.int
import scala.util.matching.Regex
import scala.util.{Failure, Success, Try}
import scala.jdk.CollectionConverters.*
import scala.language.postfixOps
import scala.math.Ordering.Implicits.infixOrderingOps

final val formatStandard = DateTimeFormatter.ofPattern("HH:mm:ss.SSS")
final val injectedRegexPattern = new Regex(Parameters.generatingPattern)

object ObtainConfigReference:
  private val config = ConfigFactory.load()
  private val logger = CreateLogger(classOf[ObtainConfigReference.type])
  private def ValidateConfig(confEntry: String):Boolean = Try(config.getConfig(confEntry)) match {
    case Failure(exception) => logger.error(s"Failed to retrieve config entry $confEntry for reason $exception"); false
    case Success(_) => true
  }

  def apply(confEntry:String): Option[Config] = if ValidateConfig(confEntry) then Some(config) else None



object CreateLogger:
  def apply[T](class4Logger: Class[T]):Logger =
    val LOGBACKXML = "logback.xml"
    val logger = LoggerFactory.getLogger(class4Logger)
    Try(getClass.getClassLoader.getResourceAsStream(LOGBACKXML)) match {
      case Failure(exception) => logger.error(s"Failed to locate $LOGBACKXML for reason $exception")
      case Success(inStream) => inStream.close()
    }
    logger


object Parameters:
  private val logger = CreateLogger(classOf[Parameters.type])
  val config = ObtainConfigReference("randomLogGenerator") match {
    case Some(value) => value
    case None => throw new RuntimeException("Cannot obtain a reference to the config data.")
  }

  //Type match is used to dependently type configuration parameter values
  //based on the default input values of the specific config parameter.
  type ConfigType2Process[T] = T match
    case Int => Int
    case Long => Long
    case String => String
    case Double => Double
    case Tuple2[Double, Double] => Tuple2[Double, Double]

  //comparing double values should be done within certain precision
  private val COMPARETHREASHOLD = 0.00001d
  implicit private val comp: Ordering[Double] = new Ordering[Double] {
    def compare(x: Double, y: Double) =
      if math.abs(x - y) <= COMPARETHREASHOLD then 0 else if x - y > COMPARETHREASHOLD then -1 else 1
  }

  private def timeoutRange: Tuple2[Long, Long] =
    val lst = Try(config.getLongList(s"randomLogGenerator.TimePeriod").asScala.toList) match {
      case Success(value) => value.sorted
      case Failure(exception) => logger.error(s"No config parameter Timeout is provided")
        throw new IllegalArgumentException(s"No config data for Timeout")
    }
    if lst.length != 2 then throw new IllegalArgumentException(s"Incorrect range of values is specified for Timeout")
    (lst(0), lst(1))
  end timeoutRange


  //for config parameter likelihood ranges, e.g., error = [0.3, 0.1], they are obtained from the conf file
  //and then sorted in the ascending order
  private def logMsgRange(logTypeName: String): Tuple2[Double, Double] =
    val lst = Try(config.getDoubleList(s"randomLogGenerator.logMessageType.$logTypeName").asScala.toList) match {
      case Success(value) => value.sorted
      case Failure(exception) => logger.error(s"No config parameter is provided: $logTypeName")
        throw new IllegalArgumentException(s"No config data for $logTypeName")
    }
    if lst.length != 2 then throw new IllegalArgumentException(s"Incorrect range of values is specified for log $logTypeName")
    (lst(0), lst(1))
  end logMsgRange

  //It returns a function that takes the name of config entry and obtains the value of this entry if it exists
  //or it logs a warning message if it is absent and returns a default value
  private def func4Parameter[T](defaultVal: T, f: String => T): String => T =
    (pName: String) => Try(f(s"randomLogGenerator.$pName")) match {
      case Success(value) => value
      case Failure(exception) => logger.warn(s"No config parameter $pName is provided. Defaulting to $defaultVal")
        defaultVal
    }
  end func4Parameter

  //in this dependently typed function a typesafe config API method is invoked
  //whose name and return value corresponds to the type of the type parameter, T
  private def getParam[T](pName: String, defaultVal: T): ConfigType2Process[T] =
    defaultVal match {
      case v: Int => func4Parameter(v, config.getInt)(pName)
      case v: Long => func4Parameter(v, config.getLong)(pName)
      case v: String => func4Parameter(v, config.getString)(pName)
      case v: Double => func4Parameter(v, config.getDouble)(pName)
      case v: Tuple2[Double, Double] => logMsgRange(pName)
    }
  end getParam

  import scala.concurrent.duration.*

  private val MINSTRINGLENGTH = 10
  private val minStrLen = getParam("MinString", MINSTRINGLENGTH)

  //these vals are the public interface of this object, so that its
  //clients can obtain typed config parameter values
  val minStringLength: Int = if minStrLen < MINSTRINGLENGTH then
    logger.warn(s"Min string length is set to $MINSTRINGLENGTH")
    MINSTRINGLENGTH
  else minStrLen
  val generatingPattern = getParam("Pattern", "([a-c][e-g][0-3]|[A-Z][5-9][f-w]){5,15}")
  val timePeriod = timeoutRange
  val timeInterval = getParam("Interval", 1)
  val startTime = getParam("StartTime", "00:00:00.000")
  val endTime = getParam("EndTime", "23:59:59.666")
  val maxCount = getParam("MaxCount", 0)
  val runDurationInMinutes: Duration = if Parameters.maxCount > 0 then Duration.Inf else getParam("DurationMinutes", 0).minutes

  if Parameters.maxCount > 0 then logger.warn(s"Max count ${Parameters.maxCount} is used to create records instead of timeouts")
  if timePeriod._1 < 0 || timePeriod._2 < 0 then throw new IllegalArgumentException("Timer period cannot be less than zero")

  //it is important to determine if likelihood ranges are not nested, otherwise
  //it would be difficult to make sense of the types of the generated log messages
  //if two types of messages have the same likehood of being generated, then this
  //likelihood conflict should be resolved. It can be, but then clients would have
  //hard time understanding why certain types of messages may appear more than other
  //types of messages, if it comes to that. It is better to inform the client about the overlap.
  private def checkForNestedRanges(input: ListMap[Tuple2[Double, Double], _]): Boolean =
    val overlaps = for {
      range1 <- input.keySet
      range2 <- input.keySet
      if (range1._1 >= range2._1 && range1._2 <= range2._2 && range1 != range2)
    } yield (range1, range2)
    if overlaps.toList.length > 0 then
      logger.error(s"Ranges of likelihoods overlap: $overlaps")
      true
    else
      false
  end checkForNestedRanges

  private val compFunc: (Tuple2[Double, Double], Tuple2[Double, Double]) => Boolean = (input1, input2) => (input1._1 <= input2._1) && (input1._2 < input2._2)
  val logRanges = ListMap(Map[Tuple2[Double, Double], String => Unit](
    getParam("info", (0.3d, 1d)) -> logger.info,
    getParam("error", (0d, 0.05d)) -> logger.error,
    getParam("warn", (0.05d, 0.15d)) -> logger.warn,
    getParam("debug", (0.15d, 0.3d)) -> logger.debug
  ).toSeq.sortWith((i1, i2) => compFunc(i1._1, i2._1)): _*)
  if checkForNestedRanges(logRanges) then throw new Exception("Overrlapping likelihood ranges will lead to the loss of precision.")


// where my code starts
// new additions

// function to add .csv to file
def addCSV(oldName: String, newName: String) =
  new File(oldName).renameTo(new File(newName))

// Produce number of characters in each log message for each log
// message type that contain the highest number of characters in the detected instances of the regex string pattern
object MapReduceLogSorter4 {
  class Map4 extends MapReduceBase with Mapper[LongWritable, Text, Text, IntWritable] :
    private val injectedRegexPattern = new Regex(Parameters.generatingPattern)


    @throws[IOException]
    def map(key: LongWritable, value: Text, output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      val line: String = value.toString
      val lines: Array[String] = line.split(" ")

      if(injectedRegexPattern.findFirstMatchIn(lines.last).isDefined) {
        output.collect(new Text(lines(2)), new IntWritable(lines.last.length))
      }


  class Reduce4 extends MapReduceBase with Reducer[Text, IntWritable, Text, IntWritable] :
    override def reduce(key: Text, values: util.Iterator[IntWritable], output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      val largest = values.asScala.max
      output.collect(key, new IntWritable(largest.get()))


  def runMapReduce4(inputPath: String, outputPath: String) =
    val conf: JobConf = new JobConf(this.getClass)
    conf.setJobName("HomeWork1")
    conf.set("fs.defaultFS", "local")
    conf.set("mapreduce.job.maps", "1")
    conf.set("mapreduce.job.reduces", "1")
    conf.setOutputKeyClass(classOf[Text])
    conf.setOutputValueClass(classOf[IntWritable])
    conf.setMapperClass(classOf[Map4])
    conf.setCombinerClass(classOf[Reduce4])
    conf.setReducerClass(classOf[Reduce4])
    conf.setInputFormat(classOf[TextInputFormat])
    conf.setOutputFormat(classOf[TextOutputFormat[Text, IntWritable]])
    FileInputFormat.setInputPaths(conf, new Path(inputPath))
    FileOutputFormat.setOutputPath(conf, new Path(outputPath))
    JobClient.runJob(conf)
    addCSV((outputPath+"/part-00000"), (outputPath+"/part-00000.csv"))
}

// For each message type you will produce the number of generated log messages
object MapReduceLogSorter3 {
  class Map3 extends MapReduceBase with Mapper[LongWritable, Text, Text, IntWritable] :
    private final val one = new IntWritable(1)
    private val word = new Text()

    @throws[IOException]
    def map(key: LongWritable, value: Text, output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      val line: String = value.toString
      line.split(" ").foreach { token =>
        if (token.matches("WARN|DEBUG|INFO|ERROR")) {
          word.set(token)
          output.collect(word, one)
        }
      }

  class Reduce3 extends MapReduceBase with Reducer[Text, IntWritable, Text, IntWritable] :
    override def reduce(key: Text, values: util.Iterator[IntWritable], output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      val sum = values.asScala.reduce((valueOne, valueTwo) => new IntWritable(valueOne.get() + valueTwo.get()))
      output.collect(key, new IntWritable(sum.get()))


  def runMapReduce3(inputPath: String, outputPath: String) =
    val conf: JobConf = new JobConf(this.getClass)
    conf.setJobName("HomeWork1")
    conf.set("fs.defaultFS", "local")
    conf.set("mapreduce.job.maps", "1")
    conf.set("mapreduce.job.reduces", "1")
    conf.set("mapreduce.output.textoutputformat.separator", ",")
    conf.setOutputKeyClass(classOf[Text])
    conf.setOutputValueClass(classOf[IntWritable])
    conf.setMapperClass(classOf[Map3])
    conf.setCombinerClass(classOf[Reduce3])
    conf.setReducerClass(classOf[Reduce3])
    conf.setInputFormat(classOf[TextInputFormat])
    conf.setOutputKeyComparatorClass(classOf[IntWritable.Comparator]) // Working Sorter
    conf.setOutputFormat(classOf[TextOutputFormat[Text, IntWritable]])
    FileInputFormat.setInputPaths(conf, new Path(inputPath))
    FileOutputFormat.setOutputPath(conf, new Path(outputPath))
    JobClient.runJob(conf)
    addCSV((outputPath+"/part-00000"), (outputPath+"/part-00000.csv"))
}

// You will compute time intervals sorted in
// the descending order that contained most log messages of type ERROR
// with injected regex pattern string instances
object MapReduceLogSorter2 {
  class Map2 extends MapReduceBase with Mapper[LongWritable, Text, Text, IntWritable] :
    private final val one = new IntWritable(1)
    private val interval = new IntWritable(Parameters.timeInterval)
    private val startTime: LocalTime = LocalTime.parse(Parameters.startTime, formatStandard) // pick up here


    @throws[IOException]
    def map(key: LongWritable, value: Text, output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      val line: String = value.toString
      val lines = line.split(" ")
      if(injectedRegexPattern.findFirstMatchIn(lines.last).isDefined && lines(2).matches("ERROR")) {
        val bucket: Int = (ChronoUnit.MINUTES.between(startTime, LocalTime.parse(lines(0), formatStandard)).toInt) / (interval.get())
        if(bucket.equals(0)) {
          output.collect(new Text(startTime.toString+" - "+startTime.plusMinutes((interval).get()).toString), one)
        } else {
          output.collect(new Text(startTime.plusMinutes((interval).get() * (bucket - 1)).toString + " - " + startTime.plusMinutes((interval).get() * bucket).toString), one)
        }
      }

  class Reduce2 extends MapReduceBase with Reducer[Text, IntWritable, Text, IntWritable] :
    override def reduce(key: Text, values: util.Iterator[IntWritable], output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      val sum = values.asScala.reduce((valueOne, valueTwo) => new IntWritable(valueOne.get() + valueTwo.get()))
      output.collect(key, new IntWritable(sum.get()))



  def runMapReduce2(inputPath: String, outputPath: String) =
    val conf: JobConf = new JobConf(this.getClass)
    conf.setJobName("HomeWork1")
    conf.set("fs.defaultFS", "local")
    conf.set("mapreduce.job.maps", "1")
    conf.set("mapreduce.job.reduces", "1")
    conf.set("mapreduce.output.textoutputformat.separator", ",")
    conf.setOutputKeyClass(classOf[Text])
    conf.setOutputValueClass(classOf[IntWritable])
    conf.setMapperClass(classOf[Map2])
    conf.setCombinerClass(classOf[Reduce2])
    conf.setReducerClass(classOf[Reduce2])
    conf.setInputFormat(classOf[TextInputFormat])
    conf.setCombinerKeyGroupingComparator(classOf[IntWritable.Comparator]) // Working Sorter
    conf.setOutputValueGroupingComparator(classOf[IntWritable.Comparator])
    conf.setOutputFormat(classOf[TextOutputFormat[Text, IntWritable]])
    FileInputFormat.setInputPaths(conf, new Path(inputPath))
    FileOutputFormat.setOutputPath(conf, new Path(outputPath))
    JobClient.runJob(conf)
    addCSV((outputPath+"/part-00000"), (outputPath+"/part-00000.csv"))
}

//
object MapReduceLogSorter1 {
  class Map1 extends MapReduceBase with Mapper[LongWritable, Text, Text, IntWritable] :

    val start: LocalTime = LocalTime.parse(Parameters.startTime, formatStandard)
    val end: LocalTime = LocalTime.parse(Parameters.endTime, formatStandard)
    private final val one = new IntWritable(1)
    private final val zero = new IntWritable(0);

    @throws[IOException]
    def map(key: LongWritable, value: Text, output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      val line: String = value.toString
      val lines: Array[String] = line.split(" ")
      val lineTime = LocalTime.parse(lines(0), formatStandard)
      if(injectedRegexPattern.findFirstMatchIn(lines.last).isDefined && start.isBefore(lineTime) && end.isAfter(lineTime)) {
        output.collect(new Text(lines(2)), one)
      } else {output.collect(new Text(lines(2)), zero)
      }


  class Reduce1 extends MapReduceBase with Reducer[Text, IntWritable, Text, IntWritable] :
    override def reduce(key: Text, values: util.Iterator[IntWritable], output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      val sum = values.asScala.reduce((valueOne, valueTwo) => new IntWritable(valueOne.get() + valueTwo.get()))
      output.collect(key, new IntWritable(sum.get()))


  def runMapReduce1(inputPath: String, outputPath: String) =
    val conf: JobConf = new JobConf(this.getClass)
    conf.setJobName("HomeWork1")
    conf.set("fs.defaultFS", "local")
    conf.set("mapreduce.job.maps", "1")
    conf.set("mapreduce.job.reduces", "1")
    conf.setOutputKeyClass(classOf[Text])
    conf.setOutputValueClass(classOf[IntWritable])
    conf.setMapperClass(classOf[Map1])
    conf.setCombinerClass(classOf[Reduce1])
    conf.setReducerClass(classOf[Reduce1])
    conf.setInputFormat(classOf[TextInputFormat])
    conf.setOutputFormat(classOf[TextOutputFormat[Text, IntWritable]])
    FileInputFormat.setInputPaths(conf, new Path(inputPath))
    FileOutputFormat.setOutputPath(conf, new Path(outputPath))
    JobClient.runJob(conf)
    addCSV((outputPath+"/part-00000"), (outputPath+"/part-00000.csv"))
}


  @main def Main(inputPath: String, outputPath: String, mode: Int): Unit = {
      mode match {
        case 1 => MapReduceLogSorter1.runMapReduce1(inputPath, outputPath)
        case 2 => MapReduceLogSorter2.runMapReduce2(inputPath, outputPath)
        case 3 => MapReduceLogSorter3.runMapReduce3(inputPath, outputPath)
        case 4 => MapReduceLogSorter4.runMapReduce4(inputPath, outputPath)
        case _ => new Success(55)
      }
  }
import com.sun.jdi.BooleanValue
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
import java.time.LocalTime
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import scala.compiletime.ops.int
import scala.util.matching.Regex
import scala.util.{Failure, Success, Try}
import scala.jdk.CollectionConverters.*
import scala.language.postfixOps
import scala.math.Ordering.Implicits.infixOrderingOps

// format constants for program
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
  val config: Config = ObtainConfigReference("logAnalysisConditions") match {
    case Some(value) => value
    case None => throw new RuntimeException("Cannot obtain a reference to the config data.")
  }

  //Type match is used to dependently type configuration parameter values
  //based on the default input values of the specific config parameter.
  type ConfigType2Process[T] = T match
    case Int => Int
    case Long => Long
    case String => String
    case Boolean => Boolean


  private def func4Parameter[T](defaultVal: T, f: String => T): String => T =
    (pName: String) => Try(f(s"logAnalysisConditions.$pName")) match {
      case Success(value) => value
      case Failure(_) => logger.warn(s"No config parameter $pName is provided. Defaulting to $defaultVal")
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
      case v: Boolean => func4Parameter(v, config.getBoolean)(pName)
    }
  end getParam

  //these vals are the public interface of this object, so that its
  //clients can obtain typed config parameter values
  val generatingPattern: ConfigType2Process[String] = getParam("Pattern", "([a-c][e-g][0-3]|[A-Z][5-9][f-w]){5,15}")
  val timeInterval: ConfigType2Process[Int] = getParam("Interval", 1)
  val startTime: ConfigType2Process[String] = getParam("StartTime", "00:00:00.000")
  val endTime: ConfigType2Process[String] = getParam("EndTime", "23:59:59.666")
  val numOfMappers: ConfigType2Process[String] = getParam("NumberOfMappers", "1")
  val numOfReducers: ConfigType2Process[String] = getParam("NumberOfReducers", "1")
  val isLocal: ConfigType2Process[Int] = getParam("Local", 1)


// function to add .csv to file
def addCSV(oldName: String): Unit =
  val end: String = Parameters.numOfReducers
  for(x <- 0 to end.toInt ) {
    val add: String = "/part-0000" + x.toString
    new File(oldName + add).renameTo(new File(oldName + add + ".csv"))
  }

// Produce number of characters in each log message for each log
// message type that contain the highest number of characters in the detected instances of the regex string pattern
object HighestNumberOfCharactersMR {
  class Map extends MapReduceBase with Mapper[LongWritable, Text, Text, IntWritable] :
    private val injectedRegexPattern = new Regex(Parameters.generatingPattern)


    @throws[IOException]
    def map(key: LongWritable, value: Text, output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      val line: String = value.toString
      val lines: Array[String] = line.split(" ")

      if(injectedRegexPattern.findFirstMatchIn(lines.last).isDefined) {
        output.collect(new Text(lines(2)), new IntWritable(lines.last.length))
      }


  class Reduce extends MapReduceBase with Reducer[Text, IntWritable, Text, IntWritable] :
    override def reduce(key: Text, values: util.Iterator[IntWritable], output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      val largest = values.asScala.max
      output.collect(key, new IntWritable(largest.get()))


  def runMapReduce(inputPath: String, outputPath: String): Unit =
    val conf: JobConf = new JobConf(this.getClass)
    conf.setJobName("HomeWork1")
    if(Parameters.isLocal.equals(1)) {
      conf.set("fs.defaultFS", "file:///")
    }
    conf.set("mapreduce.job.maps", Parameters.numOfMappers)
    conf.set("mapreduce.job.reduces", Parameters.numOfReducers)
    conf.setOutputKeyClass(classOf[Text])
    conf.setOutputValueClass(classOf[IntWritable])
    conf.setMapperClass(classOf[Map])
    conf.setCombinerClass(classOf[Reduce])
    conf.setReducerClass(classOf[Reduce])
    conf.setInputFormat(classOf[TextInputFormat])
    conf.setOutputFormat(classOf[TextOutputFormat[Text, IntWritable]])
    FileInputFormat.setInputPaths(conf, new Path(inputPath))
    FileOutputFormat.setOutputPath(conf, new Path(outputPath))
    JobClient.runJob(conf)
    addCSV(outputPath)
}

// For each message type you will produce the number of generated log messages
object TotalNumberOfLogMessagesByTypeMR {
  class Map extends MapReduceBase with Mapper[LongWritable, Text, Text, IntWritable] :
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

  class Reduce extends MapReduceBase with Reducer[Text, IntWritable, Text, IntWritable] :
    override def reduce(key: Text, values: util.Iterator[IntWritable], output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      val sum = values.asScala.reduce((valueOne, valueTwo) => new IntWritable(valueOne.get() + valueTwo.get()))
      output.collect(key, new IntWritable(sum.get()))


  def runMapReduce(inputPath: String, outputPath: String): Unit =
    val conf: JobConf = new JobConf(this.getClass)
    conf.setJobName("HomeWork1")
    if (Parameters.isLocal.equals(1)) {
      conf.set("fs.defaultFS", "file:///")
    }
    conf.set("mapreduce.job.maps", Parameters.numOfMappers)
    conf.set("mapreduce.job.reduces", Parameters.numOfReducers)
    conf.set("mapreduce.output.textoutputformat.separator", ",")
    conf.setOutputKeyClass(classOf[Text])
    conf.setOutputValueClass(classOf[IntWritable])
    conf.setMapperClass(classOf[Map])
    conf.setCombinerClass(classOf[Reduce])
    conf.setReducerClass(classOf[Reduce])
    conf.setInputFormat(classOf[TextInputFormat])
    conf.setOutputKeyComparatorClass(classOf[IntWritable.Comparator]) // Working Sorter
    conf.setOutputFormat(classOf[TextOutputFormat[Text, IntWritable]])
    FileInputFormat.setInputPaths(conf, new Path(inputPath))
    FileOutputFormat.setOutputPath(conf, new Path(outputPath))
    JobClient.runJob(conf)
    addCSV(outputPath)
}

// You will compute time intervals sorted in
// the descending order that contained most log messages of type ERROR
// with injected regex pattern string instances
object ErrorMessageInTimeIntervalMR {
  class Map extends MapReduceBase with Mapper[LongWritable, Text, Text, IntWritable] :
    private final val one = new IntWritable(1)
    private val interval = new IntWritable(Parameters.timeInterval)
    private val startTime: LocalTime = LocalTime.parse(Parameters.startTime, formatStandard) // pick up here


    @throws[IOException]
    def map(key: LongWritable, value: Text, output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      val line: String = value.toString
      val lines = line.split(" ")
      if(injectedRegexPattern.findFirstMatchIn(lines.last).isDefined && lines(2).matches("ERROR")) {
        val bucket: Int = ChronoUnit.MINUTES.between(startTime, LocalTime.parse(lines(0), formatStandard)).toInt / interval.get()
        if(bucket.equals(0)) {
          output.collect(new Text(startTime.toString+" - "+startTime.plusMinutes(interval.get()).toString), one)
        } else {
          output.collect(new Text(startTime.plusMinutes(interval.get() * (bucket - 1)).toString + " - " + startTime.plusMinutes(interval.get() * bucket).toString), one)
        }
      }

  class Reduce extends MapReduceBase with Reducer[Text, IntWritable, Text, IntWritable] :
    override def reduce(key: Text, values: util.Iterator[IntWritable], output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      val sum = values.asScala.reduce((valueOne, valueTwo) => new IntWritable(valueOne.get() + valueTwo.get()))
      output.collect(key, new IntWritable(sum.get()))



  def runMapReduce(inputPath: String, outputPath: String): Unit =
    val conf: JobConf = new JobConf(this.getClass)
    conf.setJobName("HomeWork1")
    if (Parameters.isLocal.equals(1)) {
      conf.set("fs.defaultFS", "file:///")
    }
    conf.set("mapreduce.job.maps", Parameters.numOfMappers)
    conf.set("mapreduce.job.reduces", Parameters.numOfReducers)
    conf.set("mapreduce.output.textoutputformat.separator", ",")
    conf.setOutputKeyClass(classOf[Text])
    conf.setOutputValueClass(classOf[IntWritable])
    conf.setMapperClass(classOf[Map])
    conf.setCombinerClass(classOf[Reduce])
    conf.setReducerClass(classOf[Reduce])
    conf.setInputFormat(classOf[TextInputFormat])
    conf.setCombinerKeyGroupingComparator(classOf[IntWritable.Comparator]) // Working Sorter
    conf.setOutputValueGroupingComparator(classOf[IntWritable.Comparator])
    conf.setOutputFormat(classOf[TextOutputFormat[Text, IntWritable]])
    FileInputFormat.setInputPaths(conf, new Path(inputPath))
    FileOutputFormat.setOutputPath(conf, new Path(outputPath))
    JobClient.runJob(conf)
    addCSV(outputPath)
}

//
object MessageDistributionOverTimeInterval {
  class Map extends MapReduceBase with Mapper[LongWritable, Text, Text, IntWritable] :

    val start: LocalTime = LocalTime.parse(Parameters.startTime, formatStandard)
    val end: LocalTime = LocalTime.parse(Parameters.endTime, formatStandard)
    private final val one = new IntWritable(1)
    private final val zero = new IntWritable(0)

    @throws[IOException]
    def map(key: LongWritable, value: Text, output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      val line: String = value.toString
      val lines: Array[String] = line.split(" ")
      val lineTime = LocalTime.parse(lines(0), formatStandard)
      if(injectedRegexPattern.findFirstMatchIn(lines.last).isDefined && start.isBefore(lineTime) && end.isAfter(lineTime)) {
        output.collect(new Text(lines(2)), one)
      } else {output.collect(new Text(lines(2)), zero)
      }


  class Reduce extends MapReduceBase with Reducer[Text, IntWritable, Text, IntWritable] :
    override def reduce(key: Text, values: util.Iterator[IntWritable], output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      val sum = values.asScala.reduce((valueOne, valueTwo) => new IntWritable(valueOne.get() + valueTwo.get()))
      output.collect(key, new IntWritable(sum.get()))


  def runMapReduce(inputPath: String, outputPath: String): Unit =
    val conf: JobConf = new JobConf(this.getClass)
    conf.setJobName("HomeWork1")
    if (Parameters.isLocal.equals(1)) {
      conf.set("fs.defaultFS", "file:///")
    }
    conf.set("mapreduce.job.maps", Parameters.numOfMappers)
    conf.set("mapreduce.job.reduces", Parameters.numOfReducers)
    conf.setOutputKeyClass(classOf[Text])
    conf.setOutputValueClass(classOf[IntWritable])
    conf.setMapperClass(classOf[Map])
    conf.setCombinerClass(classOf[Reduce])
    conf.setReducerClass(classOf[Reduce])
    conf.setInputFormat(classOf[TextInputFormat])
    conf.setOutputFormat(classOf[TextOutputFormat[Text, IntWritable]])
    FileInputFormat.setInputPaths(conf, new Path(inputPath))
    FileOutputFormat.setOutputPath(conf, new Path(outputPath))
    JobClient.runJob(conf)
    addCSV(outputPath)
}


  @main def Main(inputPath: String, outputPath: String, mode: Int): Unit = {
      mode match {
        case 1 => MessageDistributionOverTimeInterval.runMapReduce(inputPath, outputPath)
        case 2 => ErrorMessageInTimeIntervalMR.runMapReduce(inputPath, outputPath)
        case 3 => TotalNumberOfLogMessagesByTypeMR.runMapReduce(inputPath, outputPath)
        case 4 => HighestNumberOfCharactersMR.runMapReduce(inputPath, outputPath)
        case _ => Success(55)
      }
  }
package mining

import java.io.File
import org.apache.flink.api.java.io.TextInputFormat
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.FileProcessingMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.scala._
import org.joda.time.DateTime

case class Event(traceId: String, activity: String, start: DateTime)

case class Trace(traceId: String, events: Seq[Event])

object DirectFollower {

  val dateFormat = "yyyy/MM/dd HH:mm:ss.SSS"
  val windowsProcessingSize = 5

  def main(args: Array[String]): Unit = {
    import functions._

    val logFile = new File(getClass.getResource("/Incident.csv").getPath())

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val format = new TextInputFormat(new Path(logFile.getAbsolutePath))
    val data = env.readFile(format, logFile.getAbsolutePath, FileProcessingMode.PROCESS_CONTINUOUSLY, 1000)

    val windowsProcessing = ProcessingTimeSessionWindows.withGap(Time.seconds(windowsProcessingSize))

    env.getConfig.disableSysoutLogging()
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

    val trace = data
      .map(new EventMapFunction)
      .keyBy(0)
      .window(windowsProcessing)
      .aggregate(new TraceAggregateFunction)
      .map(new EventSortByDateMapFunction)

    val directFollower = trace
      .flatMap(new DirectFollowerFlatMapFunction)
      .keyBy(0)
      .window(windowsProcessing)
      .sum(1)

    directFollower.print()

    env.execute()
  }

}

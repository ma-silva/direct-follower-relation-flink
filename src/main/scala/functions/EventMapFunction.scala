package functions

import mining.Event
import org.apache.flink.api.common.functions.RichMapFunction
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, DateTimeZone}

class EventMapFunction extends RichMapFunction[String, Event] {

  import mining.DirectFollower.dateFormat

  override def map(value: String): Event = {
    val data = value.split(",").toList
    Event(
      data(0),
      data(1),
      DateTime.parse(data(2),
        DateTimeFormat.forPattern(dateFormat)).withZoneRetainFields(DateTimeZone.UTC)
    )
  }
}
